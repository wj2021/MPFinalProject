package fp.trajectory

import fp.Config
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.immutable.TreeSet
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * 为用户推荐兴趣区域的主要方法，包含：
 * （1）根据用户位置轨迹数据生成用户-区域事务数据
 * （2）对区域-用户事务数据进行频繁项集挖掘得到相似用户
 * （3）为每个用户推荐top-n个兴趣区域
 */
object Recommendation {
  // 根据用户位置轨迹数据生成用户-区域事务数据
  def getUserRegion(locationTrajectoryRDD: RDD[String]): RDD[(String, List[(String, Int)])] = {
    val user_regionTransactionRDD = locationTrajectoryRDD.map(c=>{
      val tokens = c.split("\t")
      (tokens(0), tokens(1).split("->").filter(!"0".equals(_)))
    }).reduceByKey(_++_).filter(_._2.length > 0).map(line=> {
      line._1 + "\t" + line._2.sortWith((a,b)=>a.toInt<b.toInt).reduce(_+","+_)
    })

    // 将 user_regionTransaction 中的region按出现次数从大到小排序并剔除重复元素
    val user_regionRDD = user_regionTransactionRDD.map(rs=>(rs.split("\t")(0), rs.split("\t")(1))).reduceByKey(_.trim+","+_.trim).map(rs=>{
      val rsMap = new scala.collection.mutable.HashMap[String, Int]()
      rs._2.split(",").foreach(e=>{
        if(rsMap.get(e).isDefined) {
          rsMap += (e->(rsMap(e)+1))
        } else rsMap += e->1
      })
      // 得到用户所有的区域列表，且按照区域出现次数从大到小排序
      (rs._1, rsMap.toList.sortWith((a,b)=>a._2>b._2))
    })
    user_regionRDD
  }

  // 对区域-用户事务数据进行频繁项集挖掘得到相似用户
  def miningFrequentPattern(region_userTransactionRDD: RDD[String]): RDD[TreeSet[String]] = {
    val minCount = Config.MIN_SUPPORT
    // transaction集合，每个transaction中的Items表示为一个集合（Set），预处理源数据，将相同的transaction计数
    val userTransactionRDD = region_userTransactionRDD.map(_.split("\t")(1).split(",").reduce(_+" "+_))
    val transactions: RDD[(TreeSet[Item],Int)] = userTransactionRDD.map(line=>{
      var transaction: TreeSet[Item] = TreeSet()
      line.split("\\s+").foreach(transaction += new Item(_))
      (transaction, 1)
    }).reduceByKey(_+_).cache()

    //计算1-频繁项集
    var K: Int = 1
    val frequentPattern: RDD[(TreeSet[Item], Int)] = userTransactionRDD.
      flatMap(_.split("\\s+")).map(word=>(TreeSet(new Item(word)),1))
      .reduceByKey(_+_).filter(_._2 >= minCount).cache()

    var result: RDD[TreeSet[String]] = frequentPattern.map(_._1.map(_.toString))

    var pattern_sets: Array[TreeSet[Item]] = frequentPattern.map(_._1).collect()
    while(pattern_sets.nonEmpty && K < 2) { // 相似用户只要求2项的频繁项集即可，不需要求3项及以上
      K += 1
      // 利用K-频繁项集生成(K+1)-候选频繁项集
      var candidate_pattern_sets: Set[TreeSet[Item]] = Set()
      for (i <- pattern_sets.indices; j <- i + 1 until pattern_sets.length) {
        if (K == 2) {
          val z: TreeSet[Item] = pattern_sets(i) ++ pattern_sets(j)
          candidate_pattern_sets += z
        } else {
          val len = pattern_sets(i).size
          if(pattern_sets(i).slice(0, len-1).equals(pattern_sets(j).slice(0, len-1))) {
            val z = pattern_sets(i) + pattern_sets(j).lastKey
            // 过滤存在不频繁子集的候选集合
            var hasInfrequentSet = false
            breakable {
              z.subsets(pattern_sets(i).size).foreach(set=>{
                if (!pattern_sets.contains(set)) {hasInfrequentSet = true; break}
              })
            }
            if(!hasInfrequentSet) candidate_pattern_sets += z
          }
        }
      }
      // 根据候选频繁项集获得频繁项集
      val bcCFI: Broadcast[Set[TreeSet[Item]]] = frequentPattern.context.broadcast(candidate_pattern_sets)
      val nextFrequentPattern = transactions.flatMap(line=>{
        var tmp: ArrayBuffer[(TreeSet[Item], Int)] = ArrayBuffer()
        bcCFI.value.foreach(itemSet=>{
          if(itemSet.subsetOf(line._1)) {
            tmp :+= Tuple2(itemSet, line._2)
          }
        })
        tmp
      }).reduceByKey(_+_).filter(_._2 >= minCount).cache()
      result = nextFrequentPattern.map(_._1.map(_.toString))
      bcCFI.unpersist()
      pattern_sets = nextFrequentPattern.map(_._1).collect()
    }
    result
  }

  // 为每个用户推荐top-n个兴趣区域
  def recommendRegion(similarUserRDD: RDD[(String, TreeSet[String])],
                      user_regionRDD: RDD[(String, List[(String, Int)])]): RDD[(String, String)] = {
    val user_regionMap: Map[String, List[(String,Int)]] = user_regionRDD.collect().toMap // 用户-区域Map，可以根据用户id很方便的查找到所访问过的区域
    val BCCURMap = user_regionRDD.context.broadcast(user_regionMap)
    val recommendRegionsRDD = similarUserRDD.map(u=>{
      val visitedRegions = BCCURMap.value(u._1) // 自己访问过的区域
      var round = 0 // 在用户-区域的Map中查找了几轮
      var count = 0 // 目前得到了多少个推荐区域
      var lastCount = 0; // 记录上一轮获取的推荐区域个数
      var recommendRegion = new TreeSet[Item]()
      var flag = true
      while(flag) {
        lastCount = count
        for (similar_user <- u._2 if flag) {
          val regions = BCCURMap.value(similar_user) // 相似用户访问过的区域
          val ri = if(round < regions.length) new Item(regions(round)._1) else null
          if(round < regions.length && !recommendRegion.contains(ri) && !visitedRegions.contains(regions(round))) {
            recommendRegion += ri
            count += 1
            if(count >= Config.TOP_N) flag = false
          }
        }
        // 经过一轮相似用户的推荐无法得到新的区域，将终止循环，即使推荐区域未达到n个
        if(lastCount == count) flag = false
        round += 1
      }
      (u._1, if(recommendRegion.nonEmpty) recommendRegion.toArray.map(_.toString).reduce(_+","+_) else "NO RECOMMENDATION")
    })
    BCCURMap.unpersist()
    recommendRegionsRDD
  }
}

class Item(val value: String) extends Comparable[Item] with Serializable {
  override def compareTo(that: Item): Int = {
    val lena = this.value.length
    val lenb = that.value.length
    if(lena == lenb) {
      this.value.compareTo(that.value)
    } else {
      lena - lenb
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case item: Item =>
        this.value.equals(item.value)
      case _ =>
        false
    }
  }

  override def toString: String = value

  override def hashCode(): Int = value.hashCode
}
