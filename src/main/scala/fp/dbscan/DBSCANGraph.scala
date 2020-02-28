package fp.dbscan

object DBSCANGraph {
  // 新建一个空图，结点->邻居表示法
  def apply[T](): DBSCANGraph[T] = new DBSCANGraph(Map[T, Set[T]]())
}

class DBSCANGraph[T] private (nodes: Map[T, Set[T]]) extends Serializable {
  // 向图中添加结点v
  def addVertex(v: T): DBSCANGraph[T] = {
    nodes.get(v) match {
      case None    => new DBSCANGraph(nodes + (v -> Set()))
      case Some(_) => this
    }
  }

  // 在结点u,v之间添加边
  def connect(u: T, v: T): DBSCANGraph[T] = {
    insertEdge(u, v).insertEdge(v, u)
  }

  def insertEdge(u: T, v: T): DBSCANGraph[T] = {
    nodes.get(u) match {
      case None       => new DBSCANGraph(nodes + (u -> Set(v)))
      case Some(neighbours) => new DBSCANGraph(nodes + (u -> (neighbours + v)))
    }
  }

  // 获取从结点u能够到达的所有结点
  def getConnected(u: T): Set[T] = {
    getAdjacent(Set(u), Set[T](), Set[T]()) - u
  }

  @scala.annotation.tailrec // 声明该方法是尾递归
  private def getAdjacent(tovisit: Set[T], visited: Set[T], adjacent: Set[T]): Set[T] = {
    tovisit.headOption match {
      case Some(current) =>
        nodes.get(current) match {
          case Some(edges) =>
            getAdjacent(edges.diff(visited) ++ tovisit.tail, visited + current, adjacent ++ edges)
          case None => getAdjacent(tovisit.tail, visited, adjacent)
        }
      case None => adjacent
    }
  }

}
