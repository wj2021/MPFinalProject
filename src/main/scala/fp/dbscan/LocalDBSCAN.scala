package fp.dbscan

import fp.dbscan.DBSCANLabeledPoint.Flag

/**
 * DBSCAN算法的实现，复杂度为O(n2)
 */
class LocalDBSCAN(eps: Double, minPoints: Int) {
  val minDistanceSquared: Double = eps * eps

  def fit(points: Iterable[DBSCANPoint]): Iterable[DBSCANLabeledPoint] = {
    val labeledPoints = points.map(new DBSCANLabeledPoint(_)).toArray
    val totalClusters = labeledPoints.foldLeft(DBSCANLabeledPoint.Unknown)((cluster, point) => {
      if(!point.visited) {
        point.visited = true
        val neighbors = findNeighbors(point, labeledPoints)
        if(neighbors.size < minPoints) {
          point.flag = Flag.Noise
          cluster
        } else {
          expandCluster(point, neighbors, labeledPoints, cluster + 1)
          cluster + 1
        }
      } else {
        cluster
      }
    })
    println(s"wj found: $totalClusters clusters")
    labeledPoints
  }

  private def findNeighbors(point: DBSCANPoint, all: Array[DBSCANLabeledPoint]): Iterable[DBSCANLabeledPoint] = {
    all.view.filter(point.distanceSquared(_) <= minDistanceSquared)
  }

  def expandCluster(point: DBSCANLabeledPoint, neighbors: Iterable[DBSCANLabeledPoint],
                    all: Array[DBSCANLabeledPoint], cluster: Int): Unit = {
    point.flag = Flag.Core
    point.cluster = cluster
    val allNeighbors = scala.collection.mutable.Queue(neighbors)
    while (allNeighbors.nonEmpty) {
      allNeighbors.dequeue().foreach(neighbor => {
        if (!neighbor.visited) {
          neighbor.visited = true
          neighbor.cluster = cluster
          val neighborNeighbors = findNeighbors(neighbor, all)
          if (neighborNeighbors.size >= minPoints) {
            neighbor.flag = Flag.Core
            allNeighbors.enqueue(neighborNeighbors)
          } else {
            neighbor.flag = Flag.Border
          }
          if (neighbor.cluster == DBSCANLabeledPoint.Unknown) {
            neighbor.cluster = cluster
            neighbor.flag = Flag.Border
          }
        }
      })
    }
  }

}
