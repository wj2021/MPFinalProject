package fp.dbscan

/**
 * 矩形，左上角(x,y)，右下角(x2,y2)
 */
case class DBSCANRectangle(x: Double, y: Double, x2: Double, y2: Double) {
  // 是否包含另一个矩形
  def contains(other: DBSCANRectangle): Boolean = {
    x <= other.x && other.x2 <= x2 && y <= other.y && other.y2 <= y2
  }

  // 点是否在矩形内（包括边界）
  def contains(point: DBSCANPoint): Boolean = {
    x <= point.x && point.x <= x2 && y <= point.y && point.y <= y2
  }

  // 点是否在矩形内（不包括边界）
  def almostContains(point: DBSCANPoint): Boolean = {
    x < point.x && point.x < x2 && y < point.y && point.y < y2
  }

  // 将矩形缩小指定长度和宽度
  def shrink(amount: Double): DBSCANRectangle = {
    DBSCANRectangle(x + amount, y + amount, x2 - amount, y2 - amount)
  }

  override def toString: String = s"[$x,$y,$x2,$y2]"

}
