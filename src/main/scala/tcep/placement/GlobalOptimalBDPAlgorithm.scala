package tcep.placement
import akka.actor.ActorContext
import akka.cluster.{Cluster, Member}
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node
import tcep.utils.SpecialStats

import scala.concurrent.Future
import scala.concurrent.duration._

object GlobalOptimalBDPAlgorithm extends PlacementStrategy {

  override val name: String = "GlobalOptimalBDP"
  private var singleNodePlacement: Option[Member] = None

  override def hasInitialPlacementRoutine(): Boolean = false

  override def hasPeriodicUpdate(): Boolean = false

  /**
    * Find optimal node for the operator to place
    *
    * @return HostInfo, containing the address of the host node
    */
  override def findOptimalNode(context: ActorContext, cluster: Cluster, dependencies: Node.Dependencies, askerInfo: HostInfo, operator: Query): Future[HostInfo] = {

    val res = for {
      init <- this.initialize(cluster)
    } yield {
      cluster.system.scheduler.scheduleOnce(5 seconds)(() => this.singleNodePlacement = None) // reset
      if (this.singleNodePlacement.isDefined) {
        SpecialStats.log(this.name, "placement", s"a previous operator has already been placed optimally on ${this.singleNodePlacement}, placing ${operator.getClass.getSimpleName} there")
        Future {
          this.updateOperatorToParentBDP(cluster, operator, this.singleNodePlacement.get, dependencies.parents)
          HostInfo(this.singleNodePlacement.get, operator, this.getPlacementMetrics(operator))
        }
      } else {
        // includes Publishers, Subscriber, and Candidates that are up
        val allNodes = findPossibleNodesToDeploy(cluster)
        // get publishers and subscribers
        val publishers = allNodes.filter(_.hasRole("Publisher")).toList
        val subscriber = allNodes.find(_.hasRole("Subscriber"))

        SpecialStats.log(this.name, "placement", s"publishers: ${publishers.size} subscriber found: ${subscriber.isDefined}")
        if (publishers.isEmpty) {
          SpecialStats.log(this.name, "placement", s"could not find publisher nodes in cluster: ${allNodes}")
          throw new RuntimeException(s"cannot find publisher nodes in cluster! \n ${cluster.state.members.mkString("\n")}")
        }
        if (subscriber.isEmpty) {
          SpecialStats.log(this.name, "placement", s"could not find subscriber node in cluster: ${allNodes}")
          throw new RuntimeException(s"cannot find subscriber node in cluster! \n ${cluster.state.members.mkString("\n")}")
        }
        else {
          SpecialStats.log(this.name, "placement", s"started requests for coords and bandwidths of ${allNodes.size} members")
          //val coordRequest: Future[Map[Member, Coordinates]] = getCoordinatesOfMembers(cluster, allNodes, Some(operator))
          //val bandwidthMeasurementsRequests: Future[Set[Map[(Member, Member), Double]]] = TCEPUtils.setOfFutureToFuture(allNodes.map(c => getAllBandwidthsFromNode(cluster, c, Some(operator))))
          for {
            candidateCoordinates <- getCoordinatesOfMembers(cluster, allNodes, Some(operator))
            foo <- Future { SpecialStats.log(this.name, "placement", s"retrieved coordinates")}
            unfilteredBw <- getAllBandwidths(cluster, Some(operator))
            bandwidthMeasurements <- Future {
              val filtered = unfilteredBw.filterKeys(k => allNodes.contains(k._1) && allNodes.contains(k._2))
              SpecialStats.log(this.name, "placement", s"retrieved bandwidths from local taskManager, ${unfilteredBw.size} pairs total; for candidates: ${filtered.size}")
              filtered
            }
            minBDPCandidate = applyGlobalOptimalBDPAlgorithm(operator, subscriber.get, publishers, candidateCoordinates, bandwidthMeasurements)
            bdpUpdate <- this.updateOperatorToParentBDP(cluster, operator, minBDPCandidate._1, dependencies.parents)
          } yield {

            val hostInfo = HostInfo(minBDPCandidate._1, operator, this.getPlacementMetrics(operator))
            placementMetrics.remove(operator) // placement of operator complete, clear entry
            this.singleNodePlacement = Some(minBDPCandidate._1)
            hostInfo
          }
        }
      }
    }
    res.flatten
  }

  /**
    * Find two optimal nodes for the operator placement, one node for hosting the operator and one backup node for high reliability
    *
    * @return HostInfo, containing the address of the host nodes
    */
  override def findOptimalNodes(context: ActorContext, cluster: Cluster, dependencies: Node.Dependencies, askerInfo: HostInfo, operator: Query): Future[(HostInfo, HostInfo)] = {
    for {
      mainNode <- findOptimalNode(context, cluster, dependencies, askerInfo, operator)
    } yield {
      val backup = HostInfo(cluster.selfMember, operator, OperatorMetrics())
      (mainNode, backup)
    }
  }

  /**
    * finds the node with minimum Bandwidth-Delay-Product sum to all publishers and the consumer, using global knowledge about
    * the latency space and bandwidths between nodes.
    * successive calls for different operators (supposedly from the same graph) yield the same host
    * publishers and subscriber can host operators only if they also have the "Candidate" role
    * @param operator operator to be placed
    * @return candidate with minimum bdp
    */
  def applyGlobalOptimalBDPAlgorithm(operator: Query, subscriber: Member, publishers: List[Member], allCoordinates: Map[Member, Coordinates], bandwidthMeasurements: Map[(Member, Member), Double]): (Member, Double) = {

    SpecialStats.log(this.name, "placement", s"received coordinates and bandwidths, applying ${this.name} with ${allCoordinates.size} candidates")
    val allLatencies: Map[(Member, Member), Double] = allCoordinates.flatMap(s => allCoordinates.map(t => (s._1, t._1) -> s._2.distance(t._2)))
    // Bandwidth-Delay-Product (BDP) between all pairs (bandwidth map does not contain keys for (m, m) -> return 0 for equal nodes)
    val linkBDPs: Map[(Member, Member), Double] = allLatencies.map(l => l._1 -> l._2 * bandwidthMeasurements.getOrElse(l._1, if(l._1._1.equals(l._1._2)) 0.0 else Double.MaxValue))
    // calculate for each candidate the BDP between it and all publishers and the subscriber
    def getBDPEntry(a: Member, b: Member): Double = linkBDPs.getOrElse((a, b), linkBDPs.getOrElse((b, a), Double.MaxValue))
    // only use candidates (if publishers and subscriber are included depends on whether they additionally have the "Candidate" role)
    val placementBDPs = allCoordinates.keySet.filter(_.hasRole("Candidate")).map(c => c -> {
      val bdpsToPublishers = publishers.map(p => getBDPEntry(c, p))
      val bdpToSubscriber = getBDPEntry(c, subscriber)
      bdpsToPublishers.sum + bdpToSubscriber
    })

    val minBDPCandidate = placementBDPs.minBy(_._2)
    SpecialStats.log(this.name, "placement", s"found candidate with min BDP: ${minBDPCandidate}")
    SpecialStats.log(this.name, "placement", s"min BDP bandwidths: ${bandwidthMeasurements.filter(_._1._1 == minBDPCandidate._1)}")
    minBDPCandidate
  }
  /*
  def findGlobalOptimalPlacement(rootOperator: Query, hosts: List[Member], allPairMetrics: Map[(Member, Member), Double],
                                 calculateMetricOfPlacement: (Map[Query, Member], Query, Map[(Member, Member), Double]) => Double,
                                 min: Boolean = true): (List[(Query, Member)], Double) = {

    val operators = this.extractOperators(rootOperator)
    val allPossiblePlacements = findAllPossiblePlacementsRec(operators.keys.toList, hosts)
    val allPlacementMetrics = allPossiblePlacements.map(p => p -> calculateMetricOfPlacement(p.toMap, rootOperator, allPairMetrics))
    if(min) allPlacementMetrics.minBy(_._2)
    else allPlacementMetrics.maxBy(_._2)
  }

  /**
    * recursively calculate all possible placements by iterating over the operators and building a list of placement alternatives for each host
    * note that the result grows according to |hosts|^|operators| , e.g. 5^8 = 390625
    * @param operators operators to be placed
    * @param hosts available hosts
    * @param currentPlacementAcc accumulator holding an incomplete placement
    * @return a list of lists (placements) containing all possible placements
    */
  def findAllPossiblePlacementsRec(operators: List[Query], hosts: List[Member], currentPlacementAcc: List[(Query, Member)] = List()): List[List[(Query, Member)]] = {
    operators match {
      // return a list of placements: for every host, an alternative placement with the current operator is generated
      case op :: remainingOps => hosts.flatMap(host => findAllPossiblePlacementsRec(remainingOps, hosts, (op, host) :: currentPlacementAcc))
      case Nil => List(currentPlacementAcc)
    }
  }

  def calculateBDPOfPlacement(placement: Map[Query, Member], rootOperator: Query, coords: Map[Member, Coordinates], bandwidths: Map[(Member, Member), Double]): Double = {

    def getBandwidth(m1: Option[Member], m2: Option[Member]): Double = {
      if(m1.isDefined && m2.isDefined) bandwidths.getOrElse((m1.get, m2.get), bandwidths.getOrElse((m2.get, m1.get), Double.MaxValue))
      else Double.MaxValue
    }
    val candidates: Set[Member] = coords.keySet.intersect(bandwidths.keySet.map(_._1)).intersect(bandwidths.keySet.map(_._2))
    if(candidates.size < coords.size || candidates.size < bandwidths.size * bandwidths.size) log.warn("")
    val opToParentBDPs = placement.map(op => op._1 -> {
      val opCoords = coords(op._2)
      val bdpToParents = op._1 match { // a parent is a subquery (sq)
        // find stream publisher by member address port (see pNames)
        case s: StreamQuery =>
          val publisherHost = candidates.find(_.address.toString.contains(s.publisherName))
          getBandwidth(Some(op._2), publisherHost) * opCoords.distance(coords.find(e => s.publisherName.contains(e._1.address.port.get.toString)).get._2)

        case s: SequenceQuery =>
          val publisher1Host = candidates.find(_.address.toString.contains(s.s1.publisherName))
          val publisher2Host = candidates.find(_.address.toString.contains(s.s2.publisherName))
          getBandwidth(Some(op._2), publisher1Host) * opCoords.distance(coords(publisher1Host.get)) +
          getBandwidth(Some(op._2), publisher2Host) * opCoords.distance(coords(publisher2Host.get))

        case b: BinaryQuery => getBandwidth(Some(op._2), placement.get(b.sq1)) * opCoords.distance(coords(placement(b.sq1))) + getBandwidth(Some(op._2), placement.get(b.sq2)) * opCoords.distance(coords(placement(b.sq2)))
        case u: UnaryQuery => getBandwidth(Some(op._2), placement.get(u.sq)) * opCoords.distance(coords(placement(u.sq)))
      }
      if(op._1 == rootOperator) {
        val subscriberCoord = coords.find(_._1.hasRole("Subscriber")).getOrElse(throw new RuntimeException(s"cannot find subscriber coords: ${coords.mkString("\n")}"))
        bdpToParents + getBandwidth(Some(op._2), Some(subscriberCoord._1) ) * opCoords.distance(subscriberCoord._2)
      }
      else bdpToParents
    })
    opToParentBDPs.values.sum
  }
  */
  override def calculateVCSingleOperator(cluster: Cluster, operator: Queries.Query, startCoordinates: Coordinates, dependencyCoordinates: List[Coordinates], nnCandidates: Map[Member, Coordinates]): Future[Coordinates] = ???

  override def selectHostFromCandidates(coordinates: Coordinates, memberToCoordinates: Map[Member, Coordinates], operator: Option[Queries.Query]): Future[Member] = ???
}
