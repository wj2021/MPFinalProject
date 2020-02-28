package fp.dbscan

import fp.dbscan.DBSCANLabeledPoint.Flag
import org.apache.spark.rdd.RDD

object DBSCANSpark {
  /**
   * 使用并行DBSCAN算法对数据进行聚类，并为每个点标上簇号
   * @param data 源数据，一系列点的集合，类型为 RDD[(Double, Double)]
   * @param eps 设置点的邻域范围
   * @param minPoints 在一个邻域内至少有 minPoints 个点，则其中心被标为核心点
   * @param maxPointsPerPartition 每个划分中点的最大个数
   */
  def train(data: RDD[(Double, Double)], eps: Double, minPoints: Int, maxPointsPerPartition: Int): DBSCANSpark = {
    new DBSCANSpark(eps, minPoints, maxPointsPerPartition, null, null).train(data)
  }
}

/**
 * 并行化DBSCAN算法的实现
 * 先将数据点进行分片，然后使用单机DBSCAN算法对每一个分片内的数据进行聚类（并行化执行每个分片），
 * 最终将每个分片的结果合并形成全局结果
 */
class DBSCANSpark private(val eps: Double, val minPoints: Int, val maxPointsPerPartition: Int,
                          @transient val partitions: List[(Int, DBSCANRectangle)],
                          @transient private val labeledPartitionedPoints: RDD[(Int, DBSCANLabeledPoint)]) extends Serializable {
  private val minimumRectangleSize: Double = 2 * eps

  def labeledPoints: RDD[DBSCANLabeledPoint] = labeledPartitionedPoints.values

  private def train(tPoints: RDD[(Double, Double)]): DBSCANSpark = {
    // 使用最小矩形切缝空间，并计算出同一个矩阵中的点数
    val minimumRectanglesWithCount = tPoints.map(toMinimumBoundingRectangle).map((_, 1))
      .aggregateByKey(0)(_+_, _+_).collect().toSet

    // 找到一个对整个空间的最好划分
    val localPartitions: List[(DBSCANRectangle, Int)] = EvenSplitPartitioner
      .partition(minimumRectanglesWithCount, maxPointsPerPartition, minimumRectangleSize)

    print("wj Found partitions: ")
    localPartitions.foreach(p => print(p.toString + ", "))
    println()

    // 将每个划分块的矩阵向外扩张 eps 大小
    val localMargins: List[((DBSCANRectangle, DBSCANRectangle, DBSCANRectangle), Int)] =
      localPartitions.map({ case (p, _) => (p.shrink(eps), p, p.shrink(-eps)) }).zipWithIndex

    val margins = tPoints.context.broadcast(localMargins)

    // 将每个点分配给合适的划分
    val duplicated = for {
      point <- tPoints.map(DBSCANPoint)
      ((_, _, outer), id) <- margins.value
      if outer.contains(point)
    } yield (id, point)

    val numOfPartitions = localPartitions.size

    // 每个划分执行DBSCAN聚类算法
    val clustered = duplicated.groupByKey(numOfPartitions).flatMapValues(points =>
      new LocalDBSCANArchery(eps, minPoints).fit(points)).cache()
//      new LocalDBSCAN(eps, minPoints).fit(points)).cache()

    // 找到所有需要合并的点
    val mergePoints = clustered.flatMap({
      case (partition, point) =>
        margins.value.filter({
          case ((inner, main, _), _) => main.contains(point) && !inner.almostContains(point)
        }).map({
          case (_, newPartition) => (newPartition, (partition, point))
        })
    }).groupByKey()

    val adjacencies = mergePoints.flatMapValues(findAdjacencies).values.collect()

    val adjacencyGraph = adjacencies.foldLeft(DBSCANGraph[(Int, Int)]()) {
      case (graph, (from, to)) => graph.connect(from, to)
    }

    // 得到所有簇id
    val localClusterIds = clustered.filter({ case (_, point) => point.flag != Flag.Noise })
      .mapValues(_.cluster).distinct().collect().toList

    // 为簇分配一个全局的id
    val (total, clusterIdToGlobalId) = localClusterIds.foldLeft((0, Map[(Int, Int), Int]())) {
      case ((id, map), clusterId) =>
        map.get(clusterId) match {
          case None =>
            val nextId = id + 1
            val connectedClusters = adjacencyGraph.getConnected(clusterId) + clusterId
            val toAdd = connectedClusters.map((_, nextId)).toMap
            (nextId, map ++ toAdd)
          case Some(_) => (id, map)
        }
    }

//    clusterIdToGlobalId.foreach(e => print(e.toString+", "))
    println(s"wj get total unique clusters: $total")

    val clusterIds = tPoints.context.broadcast(clusterIdToGlobalId)

    // 为原来分片内的数据点标上簇号
    val labeledInner = clustered.filter(isInnerPoint(_, margins.value)).map {
      case (partition, point) =>
        if (point.flag != Flag.Noise) {
          point.cluster = clusterIds.value((partition, point.cluster))
        }
        (partition, point)
    }

    // 为重叠的数据点标上簇号
    val labeledOuter = mergePoints.flatMapValues(partition => {
      partition.foldLeft(Map[DBSCANPoint, DBSCANLabeledPoint]())({
        case (all, (partition, point)) =>
          if (point.flag != Flag.Noise) {
            point.cluster = clusterIds.value((partition, point.cluster))
          }
          all.get(point) match {
            case None => all + (point -> point)
            case Some(prev) =>
              if (point.flag != Flag.Noise) {
                prev.flag = point.flag
                prev.cluster = point.cluster
              }
              all
          }
      }).values
    })

    val finalPartitions = localMargins.map { case ((_, p, _), index) => (index, p)}

    new DBSCANSpark(eps, minPoints, maxPointsPerPartition, finalPartitions, labeledInner.union(labeledOuter))

  }

  private def isInnerPoint(entry: (Int, DBSCANLabeledPoint),
                           margins: List[((DBSCANRectangle, DBSCANRectangle, DBSCANRectangle), Int)]): Boolean = {
    entry match {
      case (partition, point) =>
        val ((inner, _, _), _) = margins.filter({
          case (_, id) => id == partition
        }).head
        inner.almostContains(point)
    }
  }

  private def findAdjacencies(partition: Iterable[(Int, DBSCANLabeledPoint)]): Set[((Int, Int), (Int, Int))] = {
    val zero = (Map[DBSCANPoint, (Int, Int)](), Set[((Int, Int), (Int, Int))]())
    val (_, adjacencies) = partition.foldLeft(zero)({
      case ((seen, adjacencies), (partition, point)) =>
        if (point.flag == Flag.Noise) {
          (seen, adjacencies)
        } else {
          val clusterId = (partition, point.cluster)
          seen.get(point) match {
            case None                => (seen + (point -> clusterId), adjacencies)
            case Some(prevClusterId) => (seen, adjacencies + ((prevClusterId, clusterId)))
          }
        }
    })
    adjacencies
  }

  private def toMinimumBoundingRectangle(p: (Double, Double)): DBSCANRectangle = {
    val dp = DBSCANPoint(p)
    val x = corner(dp.x)
    val y = corner(dp.y)
    DBSCANRectangle(x, y, x + minimumRectangleSize, y + minimumRectangleSize)
  }

  private def corner(p: Double): Double = {
    val x = if(p < 0) p-minimumRectangleSize else p
    (x / minimumRectangleSize).intValue * minimumRectangleSize
  }

}
