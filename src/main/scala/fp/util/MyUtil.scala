package fp.util

/**
 * 工具类，包含3个方法：
 * （1）根据经纬度计算两点之间的距离
 * （2）将地理坐标经纬度转化为平面坐标
 * （3）计算两个时间之间的差值
 */
object MyUtil {
  val EARTH_RADIUS = 6378137 // 赤道半径(单位m)

  // 转化为弧度(rad)
  private def rad(d: Double): Double = d * Math.PI / 180.0

  /**
   * 基于googleMap中的算法计算两经纬度之间的距离,计算精度与谷歌地图的距离精度差不多，相差范围在0.2米以下（单位m）
   * @param lat1 第一点的纬度
   * @param lon1 第一点的经度
   * @param lat2 第二点的纬度
   * @param lon2 第二点的经度
   * @return 返回的距离，单位米
   */
  def GetDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val radLat1 = rad(lat1)
    val radLat2 = rad(lat2)
    val a = radLat1 - radLat2
    val b = rad(lon1) - rad(lon2)
    var d = 2 * EARTH_RADIUS * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) +
      Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
    d = (d * 10000).round / 10000
    d
  }

  /**
   * 使用米勒投影法将经纬度转化为平面坐标（单位m）
   * @param lat 维度
   * @param lon 经度
   * @return 平面坐标(x,y)
   */
  def MillierConvert(lat: Double, lon: Double): (Double, Double) = {
    val L = EARTH_RADIUS * Math.PI * 2                      // 地球周长
    val W = L                                               // 面展开后，x轴等于周长
    val H = L / 2                                           // y轴约等于周长一半
    val mill = 2.3                                          // 米勒投影中的一个常数，范围大约在正负2.3之间
    var x = rad(lon)                                        // 将经度从度数转换为弧度
    var y = rad(lat)                                        // 将纬度从度数转换为弧度
    y = 1.25 * Math.log(Math.tan(0.25 * Math.PI + 0.4 * y)) // 米勒投影的转换
    x = (W / 2) + (W / (2 * Math.PI)) * x                   // 弧度转为实际距离
    y = (H / 2) - (H / (2 * mill)) * y
    (x, y)
//    ((lat-39.9)*110000, (lon-116.3)*85000) // 设原点的地理位置为(39.9 116.3) // 如果都是北京的点只需要一行代码即可转换
  }

  /**
   * 计算两个时间之差
   * @param a 第一个时间字符串，格式为 yyyy-MM-dd HH:mm:ss
   * @param b 第二个时间字符串，格式为 yyyy-MM-dd HH:mm:ss
   * @return 秒
   */
  def getBetweenSeconds(a: String, b: String): Integer = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var startDate: java.util.Date = null
    var endDate: java.util.Date = null
    try {
      startDate = format.parse(a)
      endDate = format.parse(b)
      val ss = Math.abs(endDate.getTime - startDate.getTime)
      Integer.valueOf((ss / 1000).toInt)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        0
    }
  }

}
