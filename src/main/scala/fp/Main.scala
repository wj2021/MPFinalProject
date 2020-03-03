package fp

import fp.trajectory.{Recommendation, TrajectoryProcess}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import pre.DataPreprocess

import scala.collection.immutable.TreeSet

/**
 * 挖掘轨迹数据，并为用户推荐兴趣区域，具体步骤如下：
 * 1.预处理原始轨迹数据，处理后的数据每一行数据格式为：uid\tp1->p2->...pn，
 *   其中uid为用户id，pi为一个轨迹点，由纬度,经度,时间表示
 * 2.从轨迹数据中提取停留点信息，如果一连串的轨迹点之间的距离小于阈值，但停留时间却超过阈值，
 *   则这多个轨迹点组成一个停留区域，停留区域的坐标为区域内所有轨迹点的平均值。
 *   停留点的数据格式为：uid\ts1->s2->...->sn，其中si表示一个停留点，由纬度,经度,进入时间，离开时间组成
 * 3.使用DBSCAN算法将停留点进行聚类，生成许多的簇，将每个停留点标上簇号
 * 4.在知道每个停留点的簇号后，将用户的停留点轨迹转化为位置轨迹，即使用簇号来代表原来停留点轨迹中的停留点，
 *   位置轨迹表示为：uid\tc1->c2->...->cn，其中ci是停留点所在的簇号
 * 5.根据位置轨迹生成用户-区域事务数据，即每一行表示一个用户所访问的位置集合，按访问次数小大到小排列，不同行代表不同用户
 * 6.根据用户-区域事务数据生成区域-用户事务数据，即每一行表示一个区域所到访的所有用户id
 * 7.使用频繁项集挖掘算法挖掘区域-用户事务数据，得到相似用户列表，如果有两个用户都访问过多个相同区域，则可定义这两个用户为相似用户
 * 8.根据相似用户信息和用户-区域事务数据，为用户推荐兴趣区域，兴趣区域定义为相似用户访问过且自己未访问的区域
 */
object Main {
  def main(args: Array[String]): Unit = {
    // 删除输出目录
    val path = new Path(args(2))
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    if(hdfs.exists(path)) {
      hdfs.delete(path, true)
    }

    // 创建spark配置
    val conf = new SparkConf()
//    conf.setMaster("local[2]")
    conf.setAppName("MiningTrajectoryData")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // 1.预处理数据
    val src = args(0) // 源轨迹数据目录
    val dst = args(1) // 处理后数据保存目录
    DataPreprocess.processing(src, dst, hdfs)

    // 2.从轨迹数据中提取停留点信息
    val trajectoryRDD :RDD[String] = sc.textFile(dst) // 从文件中读取用户轨迹数据
    val stayPointsTrajectoryRDD :RDD[String] = TrajectoryProcess.getStayPoints(trajectoryRDD).cache()
    stayPointsTrajectoryRDD.saveAsTextFile(args(2)+"/0_stayPointsTrajectory")

    // 3.使用DBSCAN算法将停留点进行聚类，并为每个停留点标记簇号
    val parseDataRDD = stayPointsTrajectoryRDD.flatMap(_.split("\t")(1).split("->")).map(
      p=>(p.split(",")(0).toDouble, p.split(",")(1).toDouble)).cache()
    val model = dbscan.DBSCANSpark.train(parseDataRDD, Config.EPS, Config.MIN_POINTS, Config.MAX_POINTS_PER_PARTITION)
    model.labeledPoints.saveAsTextFile(args(2)+"/1_labeledPoints") // 保存标记好簇号的停留点

    // 4.将用户的停留点轨迹转化为位置轨迹
    val labeledPoints = sc.textFile(args(2)+"/1_labeledPoints") // 不能直接使用 model.labeledPoints，否则报错
    val locationTrajectoryRDD: RDD[String] = TrajectoryProcess.getLocationTrajectory(stayPointsTrajectoryRDD, labeledPoints)
    locationTrajectoryRDD.saveAsTextFile(args(2)+"/2_locationTrajectory") // 保存位置轨迹信息

    // 5.根据位置轨迹生成用户-区域事务数据，且区域按照出现次数从大到小排列
    val user_regionRDD: RDD[(String, List[(String, Int)])] = Recommendation.getUserRegion(locationTrajectoryRDD).cache()
    user_regionRDD.map(one=>one._1+"\t"+one._2.map(_._1).reduce(_+","+_)).saveAsTextFile(args(2)+"/3_user_region")

    // 6.根据用户-区域事务数据生成区域-用户事务数据
    val region_userTransactionRDD: RDD[String] = user_regionRDD.flatMap(ur=>{
      var region_users: Array[(String, String)] = Array()
      val regions = ur._2
      for(r <- regions) region_users :+= (r._1, ur._1)
      region_users
    }).reduceByKey(_+","+_).map(ru=>ru._1 + "\t" + ru._2.split(",").distinct.sortWith((a,b)=>a.toInt<b.toInt).reduce(_+","+_))
    region_userTransactionRDD.saveAsTextFile(args(2)+"/4_region_user")

    // 7.频繁项集挖掘得到相似用户
    val similarUserRDD: RDD[(String, TreeSet[String])] = Recommendation.miningFrequentPattern(region_userTransactionRDD).flatMap(us=>{
      var r: Array[(String, TreeSet[String])] = Array()
      us.foreach(u=>r :+= (u, us-u))
      r
    }).reduceByKey(_++_)
    similarUserRDD.map(one=>one._1+"\t"+one._2.reduce(_+","+_)).repartition(2).saveAsTextFile(args(2)+"/5_similarUser")

    // 8.为用户推荐兴趣区域
    val recommendRegions = Recommendation.recommendRegion(similarUserRDD, user_regionRDD)
    recommendRegions.map(rr=>s"${rr._1}\t${rr._2}").saveAsTextFile(args(2)+"/6_recommendRegions") // 保存用户推荐兴趣区域

    sc.stop()
  }
}
