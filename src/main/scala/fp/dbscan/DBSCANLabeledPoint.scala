package fp.dbscan

object DBSCANLabeledPoint {
  val Unknown = 0

  object Flag extends Enumeration {
    type Flag = Value
    val Border, Core, Noise, NotFlagged = Value
  }
}

class DBSCANLabeledPoint(p :(Double, Double)) extends DBSCANPoint(p: (Double, Double)) {
  def this(dp: DBSCANPoint) = this(dp.point) // 使用 DBSCANPoint 构造 DBSCANLabeledPoint

  var flag: DBSCANLabeledPoint.Flag.Value = DBSCANLabeledPoint.Flag.NotFlagged
  var cluster: Int = DBSCANLabeledPoint.Unknown
  var visited = false

  override def toString: String = {
    s"$p\t$cluster,$flag"
  }
}