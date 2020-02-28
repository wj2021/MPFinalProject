package fp.dbscan

import fp.util.MyUtil

case class DBSCANPoint(point: (Double, Double)) {
  // 将地理坐标转化为平面坐标，在DBSCAN算法中使用平面坐标代替地理坐标进行计算
  private val point2D = MyUtil.MillierConvert(point._1, point._2)
  val x: Double = point2D._1
  val y: Double = point2D._2

  // 计算转化为平面坐标后两个地理点之间的距离
  def distanceSquared(other: DBSCANPoint): Double = {
    (x-other.x) * (x-other.x) + (y-other.y) * (y-other.y)
  }
}