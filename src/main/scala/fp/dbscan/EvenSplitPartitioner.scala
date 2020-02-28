package fp.dbscan

/**
 * 将源数据空间进行划分
 */
object EvenSplitPartitioner {
  def partition(toSplit: Set[(DBSCANRectangle, Int)], maxPointsPerPartition: Long,
                minimumRectangleSize: Double): List[(DBSCANRectangle, Int)] = {
    new EvenSplitPartitioner(maxPointsPerPartition, minimumRectangleSize).findPartitions(toSplit)
  }
}

class EvenSplitPartitioner(maxPointsPerPartition: Long, minimumRectangleSize: Double) {
  def findPartitions(toSplit: Set[(DBSCANRectangle, Int)]): List[(DBSCANRectangle, Int)] = {
    def pointsIn: DBSCANRectangle => Int = pointsInRectangle(toSplit, _: DBSCANRectangle)

    val boundingRectangle = findBoundingRectangle(toSplit)
    val toPartition = List((boundingRectangle, pointsIn(boundingRectangle)))
    val partitioned = List[(DBSCANRectangle, Int)]()

    partition(toPartition, partitioned, pointsIn).filter(_._2 > 0)
  }

  @scala.annotation.tailrec
  private def partition(remaining: List[(DBSCANRectangle, Int)], partitioned: List[(DBSCANRectangle, Int)],
                        pointsIn: DBSCANRectangle => Int): List[(DBSCANRectangle, Int)] = {
    remaining match {
      case (rectangle, count) :: rest =>
        if (count > maxPointsPerPartition) {
          if (canBeSplit(rectangle)) {
            def cost: DBSCANRectangle => Int = (r: DBSCANRectangle) => ((pointsIn(rectangle) / 2) - pointsIn(r)).abs
            val (split1, split2) = split(rectangle, cost)
            val s1 = (split1, pointsIn(split1))
            val s2 = (split2, pointsIn(split2))
            partition(s1 :: s2 :: rest, partitioned, pointsIn)
          } else {
            partition(rest, (rectangle, count) :: partitioned, pointsIn)
          }
        } else {
          partition(rest, (rectangle, count) :: partitioned, pointsIn)
        }
      case Nil => partitioned
    }

  }

  def split(rectangle: DBSCANRectangle, cost: DBSCANRectangle => Int): (DBSCANRectangle, DBSCANRectangle) = {
    val smallestSplit = findPossibleSplits(rectangle).reduceLeft { (smallest, current) =>
      if (cost(current) < cost(smallest)) {
        current
      } else {
        smallest
      }
    }
    (smallestSplit, complement(smallestSplit, rectangle))
  }

  // 求 box 矩阵在 boundary 矩阵中的补矩阵
  private def complement(box: DBSCANRectangle, boundary: DBSCANRectangle): DBSCANRectangle =
    if (box.x == boundary.x && box.y == boundary.y) { // 左上角相同
      if (boundary.x2 >= box.x2 && boundary.y2 >= box.y2) { // boundary矩形右下角大于等于box
        if (box.y2 == boundary.y2) { // 右下角y值相等
          DBSCANRectangle(box.x2, box.y, boundary.x2, boundary.y2)
        } else if (box.x2 == boundary.x2) { // 右下角x值相等
          DBSCANRectangle(box.x, box.y2, boundary.x2, boundary.y2)
        } else {
          throw new IllegalArgumentException("rectangle is not a proper sub-rectangle")
        }
      } else {
        throw new IllegalArgumentException("rectangle is smaller than boundary")
      }
    } else {
      throw new IllegalArgumentException("unequal rectangle")
    }

  // 给出一个矩形所有的可能划分
  private def findPossibleSplits(box: DBSCANRectangle): Set[DBSCANRectangle] = {
    val xSplits = (box.x + minimumRectangleSize) until box.x2 by minimumRectangleSize
    val ySplits = (box.y + minimumRectangleSize) until box.y2 by minimumRectangleSize
    val splits = xSplits.map(x => DBSCANRectangle(box.x, box.y, x, box.y2)) ++
      ySplits.map(y => DBSCANRectangle(box.x, box.y, box.x2, y))
    splits.toSet
  }

  // 给定矩形能否被划分为至少2个最小矩形
  private def canBeSplit(box: DBSCANRectangle): Boolean = {
    box.x2 - box.x > minimumRectangleSize * 2 || box.y2 - box.y > minimumRectangleSize * 2
  }

  def pointsInRectangle(space: Set[(DBSCANRectangle, Int)], rectangle: DBSCANRectangle): Int = {
    space.view.filter({case (current, _) => rectangle.contains(current)}).foldLeft(0) {
      case (total, (_, count)) => total + count
    }
  }

  def findBoundingRectangle(rectanglesWithCount: Set[(DBSCANRectangle, Int)]): DBSCANRectangle = {
    val invertedRectangle = DBSCANRectangle(Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)
    rectanglesWithCount.foldLeft(invertedRectangle) {
      case (bounding, (c, _)) =>
        DBSCANRectangle(bounding.x.min(c.x), bounding.y.min(c.y), bounding.x2.max(c.x2), bounding.y2.max(c.y2))
    }
  }

}
