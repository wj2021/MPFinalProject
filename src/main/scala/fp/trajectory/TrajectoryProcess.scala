package fp.trajectory

import fp.Config
import fp.util.MyUtil
import org.apache.spark.rdd.RDD

/**
 * 对轨迹数据进行处理的主要方法，包含：
 * （1）将原始轨迹数据转换为停留点数据
 * （2）根据簇信息，将停留点轨迹转换为位置轨迹
 */
object TrajectoryProcess {
  // 将轨迹数据转化为停留点数据
  def getStayPoints(trajectoryRDD: RDD[String]): RDD[String] = {
    val stayPointsTrajectoryRDD :RDD[String] = trajectoryRDD.map(trajectory=> {
      val stayPoints :StringBuffer = new StringBuffer()
      val trajectoryData = trajectory.split("\t")
      val userId = trajectoryData(0)
      val points = trajectoryData(1).split("->")
      var (i,j) = (0, 1)
      var (latSum,lonSum :Double) = (points(0).split(",")(0).toDouble, points(0).split(",")(1).toDouble)
      while(i < points.length && j < points.length) {
        var tokens1 = points(i).split(",") // tokens1(0):经度 tokens1(1):纬度  tokens1(2):记录轨迹点的时间
        var tokens2 = points(j).split(",")
        latSum += tokens2(0).toDouble
        lonSum += tokens2(1).toDouble
        while(j < points.length && MyUtil.GetDistance(tokens1(0).toDouble, tokens1(1).toDouble, tokens2(0).toDouble, tokens2(1).toDouble) <= Config.DISTANCE_THRESH) {
          j += 1
          if(j < points.length) {
            tokens2 = points(j).split(",")
            latSum += tokens2(0).toDouble
            lonSum += tokens2(1).toDouble
          }
        }
        // p(i)->..-.->p(j-1) 可能是停留点，还要判断时间阈值
        if(j-i > 1 && MyUtil.getBetweenSeconds(tokens1(2), points(j-1).split(",")(2)) >= Config.TIME_THRESH) {
          // 从 p(i)->...->p(j-1) 是停留点
          if(j < points.length) {
            // j未出界，经纬度和应减去j的经纬度
            latSum -= tokens2(0).toDouble
            lonSum -= tokens2(1).toDouble
          }
          // 停留点的坐标用停留区域内所有轨迹点的平均经纬度表示
          stayPoints.append(latSum/(j-i) + "," + lonSum/(j-i) + "," + tokens1(2) + "," + points(j-1).split(",")(2)).append("->")
          i = j
          j = i+1
          latSum = tokens2(0).toDouble
          lonSum = tokens2(1).toDouble
        } else {
          do{
            i += 1
            if(i < points.length) tokens1 = points(i).split(",")
          } while(i < j && MyUtil.GetDistance(tokens1(0).toDouble, tokens1(1).toDouble, tokens2(0).toDouble, tokens2(1).toDouble) > Config.DISTANCE_THRESH)
          j=i+1
          latSum = tokens1(0).toDouble
          lonSum = tokens1(1).toDouble
        }
      } // 找到17391个停留点，运行大约4分30秒

      if(stayPoints.length() > 0) {
        // 在停留点序列前面标上用户名
        stayPoints.insert(0, userId+"\t")
        // 去除最后多余的 -> 符号
        stayPoints.replace(stayPoints.length()-2, stayPoints.length(), "")
      }
      stayPoints.toString
    }).filter(_.length() > 0)
    stayPointsTrajectoryRDD
  }

  // 根据标号的停留点信息，将停留点轨迹转化为位置轨迹，即用簇的标号代替停留点
  def getLocationTrajectory(stayPointsTrajectoryRDD: RDD[String], labeledPointsRDD: RDD[String]): RDD[String] = {
    // 用map保存点和点所在的簇号
    val labeledPointsMap: Map[String, String] = labeledPointsRDD.map(p=>(p.split("\\s+")(0), p.split("\\s+")(1).split(",")(0))).collect().toMap
    val bccLP = labeledPointsRDD.context.broadcast(labeledPointsMap)
    val locationTrajectory: RDD[String] = stayPointsTrajectoryRDD.map(tra=>{
      val lt = new StringBuffer()
      val data = tra.split("\t")
      val userId = data(0)
      val sps = data(1).split("->")
      lt.append(userId).append("\t")
      var lastCluster = ""
      for(s <- sps) {
        val tokens = s.split(",")
        val p = s"(${tokens(0)},${tokens(1)})"
        val c = bccLP.value getOrElse (p, "0")
        if(!lastCluster.equals(c)) {
          lt.append(c).append("->")
          lastCluster = c
        }
      }
      lt.replace(lt.length()-2, lt.length(), "")
      lt.toString
    })
    bccLP.unpersist()
    locationTrajectory
  }


}
