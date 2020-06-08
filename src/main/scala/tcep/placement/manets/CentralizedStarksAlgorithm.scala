package tcep.placement.manets

/**
  * Created by mac on 10/10/2017.
  */

/*
object CentralizedStarksAlgorithm extends PlacementStrategy {

  //TODO: move common code in base class

  val log = LoggerFactory.getLogger(getClass)
  private var coordinatesMap = Map.empty[Member, Long]

  private val refreshInterval = ConfigFactory.load().getInt("constants.coordinates-refresh-interval")
  private val ex = new ScheduledThreadPoolExecutor(1)

  def strategyName():String  = "CentralizedStarks"

  val task = new Runnable {
    def run() = {
      coordinatesMap = coordinatesMap.empty //removing old data to force system to refresh coordinatesMap
    }
  }

  ex.scheduleAtFixedRate(task, refreshInterval, refreshInterval, TimeUnit.SECONDS)

  def findCoordinatesOfMembers(context: ActorContext, members: SortedSet[Member]): Map[Member, CoordinatesResponse] = {
    var result: Map[Member, Long] = Map.empty[Member, Long]

    for (member <- members) {
      if (coordinatesMap.contains(member)) {
        result += (member -> coordinatesMap(member))
      } else {
        val coordinates = findCoordinatesFromActorPath(context, RootActorPath(member.address) / "user" / "TaskManager")
        result += (member -> coordinates)
        coordinatesMap += (member -> coordinates)
      }
    }
    result
  }

  def findOptimalNode(context: ActorContext, cluster: Cluster, dependencies: Seq[ActorRef], myCoordinates: Long, askerInfo: NodeInfo): NodeInfo = {
    val startTime = System.currentTimeMillis()
    val members = findPossibleNodesToDeploy(cluster)
    log.info(s"the number of available members are: ${members.size}")

    val memberCoordinates = findCoordinatesOfMembers(context, members)
    val neighbors = getNeighbors(memberCoordinates.size, memberCoordinates)

    val producersCords = findVivaldiCoordinatesOfNodes(dependencies)

    val res = applyStarksAlgorithm(context, cluster, myCoordinates, producersCords, neighbors, NodeInfo(askerInfo.addr, hops = 1))
    SpecialStats.log(strategyName(), "Placement", s"Placement Time ${System.currentTimeMillis() - startTime} millis")
    SpecialStats.debug(strategyName(), s"Placement Time ${System.currentTimeMillis() - startTime} millis")

    SpecialStats.log(strategyName(), "PlacementOverhead", s"$localMsgOverhead")
    totalMessageOverhead += localMsgOverhead
    SpecialStats.log(strategyName(), "total_overhead", s"$totalMessageOverhead")
    localMsgOverhead = 0

    res
  }

  def findOptimalNodes(context: ActorContext, cluster: Cluster, dependencies: Seq[ActorRef], myCoordinates: Long, askerInfo: NodeInfo): (NodeInfo, NodeInfo) = {
    throw new RuntimeException("Starks algorithm donot support reliability")
  }

  /**
    * Applies Starks's algorithm to find the optimal node for operator deployment
    *
    * @param producers      producers nodes on which this operator is dependent
    * @param candidateNodes coordinates of the candidate nodes
    * @return the address of member where operator will be deployed
    */
  def applyStarksAlgorithm(context: ActorContext,
                           cluster: Cluster,
                           mycords: Long,
                           producers: Map[ActorRef, Long],
                           candidateNodes: Map[Member, Long], nodeInfo: NodeInfo): NodeInfo = {

    def sortNeighbours(lft: (Member, Long), rght: (Member, Long)) = {
      findDistanceFromProducers(lft._2) < findDistanceFromProducers(rght._2)
    }

    def findDistanceFromProducers(memberCord: Long): Long = {
      producers.values.foldLeft(0l)((t, c) => t + Math.abs(memberCord - c))
    }

    SpecialStats.debug(strategyName(), s"my coordinates: $mycords")
    SpecialStats.debug(strategyName(), s"neighbours: $candidateNodes")
    SpecialStats.debug(strategyName(), s"producers $producers")

    val neighbours = ListMap(candidateNodes.toSeq.sortWith(sortNeighbours): _*)

    SpecialStats.debug(strategyName(), s"sorted neighbours: $neighbours")
    val neighborDist = findDistanceFromProducers(neighbours.head._2)
    val mydist = findDistanceFromProducers(mycords)
    SpecialStats.debug(strategyName(), s"my distance from producers $mydist neighbour distance: $neighborDist")

    if (neighbours.nonEmpty && neighborDist < mydist) {
      SpecialStats.debug(strategyName(), s"neighbour will host the operator ${neighbours.head._1.address}")
      SpecialStats.debug(strategyName(), s"mine distance from producers ${findDistanceFromProducers(mycords)}, neighbours ${neighbours.head._2}")
      nodeInfo.hops += 1
      nodeInfo.distance += neighbours.head._2

      NodeInfo(neighbours.head._1.address, nodeInfo.distance, 1)
    } else {
      SpecialStats.debug(strategyName(), s"i'm hosting the operator: ${cluster.selfAddress}")
      NodeInfo(cluster.selfAddress, nodeInfo.distance, nodeInfo.hops)
    }
  }


}
*/