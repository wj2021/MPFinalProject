package fp

/**
 * 参数配置类，保存程序中使用的参数
 */
object Config {
  val DISTANCE_THRESH = 200.0 // 计算停留点时设置的距离阈值，单位米
  val TIME_THRESH = 1200 // 计算停留点时设置的时间阈值，单位秒

  // 并行化DBSCAN算法的参数
  val EPS = 100 // 在点周围100m的范围内搜寻其它点
  val MIN_POINTS = 4 // 表示一个区域有多少个点时其中心点成为核心点
  val MAX_POINTS_PER_PARTITION = 10000 // 在 DBSCAN 算法中每个分区的最大点数

  // 推荐算法参数
  val MIN_SUPPORT = 3 // 区域-用户频繁项集挖掘中的支持度计数，如果两个用户出现在相同的 MIN_SUPPORT 个区域则定义这两个用户是相似用户
  val TOP_N = 6 // 为用户推荐兴趣区域的个数
}
