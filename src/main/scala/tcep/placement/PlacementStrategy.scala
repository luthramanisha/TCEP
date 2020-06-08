package tcep.placement

import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{ActorContext, ActorRef, Address}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.{Coordinates, DistVivaldiActor}
import org.slf4j.LoggerFactory
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.machinenodes.helper.actors._
import tcep.placement.manets.StarksAlgorithm
import tcep.placement.mop.RizouAlgorithm
import tcep.placement.sbon.PietzuchAlgorithm
import tcep.utils.TCEPUtils.makeMapFuture
import tcep.utils.{SizeEstimator, SpecialStats, TCEPUtils}

import scala.collection.mutable
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Created by raheel on 17/08/2017.
  * Updated by Niels on 26/11/2018
  */
trait PlacementStrategy {

  val iterations = ConfigFactory.load().getInt("constants.placement.relaxation-initial-iterations")
  implicit val resolveTimeout = Timeout(ConfigFactory.load().getInt("constants.placement.placement-request-timeout"), TimeUnit.SECONDS)
  val requestTimeout = Duration(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS)
  val log = LoggerFactory.getLogger(getClass)
  var cluster: Cluster = _
  var placementMetrics: mutable.Map[Query, OperatorMetrics] = mutable.Map()
  var publishers: Map[String, ActorRef] = Map()
  var memberBandwidths = Map.empty[(Member, Member), Double] // cache values here to prevent asking taskManager each time (-> msgOverhead)
  var memberCoordinates = Map.empty[Member, Coordinates]
  var memberLoads = mutable.Map.empty[Member, Double] // cache loads, evict entries if an operator is deployed
  private val defaultLoad = ConfigFactory.load().getDouble("constants.default-load")
  private val defaultBandwidth = ConfigFactory.load().getDouble("constants.default-data-rate")
  private val refreshInterval = ConfigFactory.load().getInt("constants.coordinates-refresh-interval")
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  //private var bwCounter: Long = 0
  protected var initialized = false
  var initialized_on_node = "none"
  private lazy val coordRequestSize: Long = SizeEstimator.estimate(CoordinatesRequest(Address("tcp", "tcep", "speedPublisher", 1)))

  val name: String
  def hasInitialPlacementRoutine(): Boolean
  def hasPeriodicUpdate(): Boolean

  /**
    * Find optimal node for the operator to place
    *
    * @return HostInfo, containing the address of the host node
    */
  def findOptimalNode(context: ActorContext, cluster: Cluster, dependencies: Dependencies, askerInfo: HostInfo, operator: Query): Future[HostInfo]

  /**
    * Find two optimal nodes for the operator placement, one node for hosting the operator and one backup node for high reliability
    *
    * @return HostInfo, containing the address of the host nodes
    */
  def findOptimalNodes(context: ActorContext, cluster: Cluster, dependencies: Dependencies, askerInfo: HostInfo, operator: Query): Future[(HostInfo, HostInfo)]


  /**
    * starting point of the initial placement
    * retrieves the coordinates of dependencies and calculates an initial placement of all operators in a query
    * (without actually placing them yet)
    * @param cluster
    * @param query the operator graph to be placed
    * @return a map from operator to host
    */
  def initialVirtualOperatorPlacement(cluster: Cluster, query: Query): Future[Map[Query, Coordinates]] = {

    val clientNode = cluster.state.members.filter(m => m.roles.contains("Subscriber"))
    assert(clientNode.size == 1, "there must be exactly one client node with role Subscriber")
    // note that the message overhead is attributed only to the root operator since it is retrieved only once!
    val publisherCoordRequest = getCoordinatesOfNodes(cluster, publishers.values.toSeq, Some(query))
    val candidateCoordRequest = getCoordinatesOfMembers(cluster, findPossibleNodesToDeploy(cluster), Some(query))
    val virtualOperatorPlacementRequest = for {
      publisherCoordinates <- publisherCoordRequest
      candidateCoordinates <- candidateCoordRequest
      clientCoordinates <- getCoordinatesOfNode(cluster, clientNode.head, Some(query))
    } yield {
      calculateVirtualPlacementWithCoords(cluster, query, clientCoordinates, publisherCoordinates.map(e => publishers.find(_._2 == e._1).get._1 -> e._2), candidateCoordinates)
    }

    virtualOperatorPlacementRequest.onComplete {
      case Success(_) => SpecialStats.debug(s"$this", s"calculated virtual coordinates for query")
      case Failure (exception) => SpecialStats.debug(s"$this", s"calculation of virtual coordinates failed, cause: \n $exception")
    }
    virtualOperatorPlacementRequest
  }

  def calculateVCSingleOperator(cluster: Cluster, operator: Query, startCoordinates: Coordinates, dependencyCoordinates: List[Coordinates], nnCandidates: Map[Member, Coordinates]): Future[Coordinates]
  /**
    * performs an initial placement of all operators in a query by performing successive (virtual) placement updates for
    * each operator in the virtual coordinate space
    * @param query the query consisting of the operators to be placed
    */
  def calculateVirtualPlacementWithCoords(cluster: Cluster, query: Query,
                                          clientCoordinates: Coordinates,
                                          publisherCoordinates: Map[String, Coordinates],
                                          nnCandidateCoordinates: Map[Member, Coordinates]): Map[Query, Coordinates] = {

    // use LinkedHashMap here to guarantee that parent operators are updated before children
    val operatorDependencyMap = extractOperators(query, None, mutable.LinkedHashMap[Query, QueryDependencies]())
    val operators = operatorDependencyMap.toList.map(_._1).reverse // reverse so that parents are updated before their children
    var currentOperatorCoordinates: Map[Query, Coordinates] = operators.map(o => o -> Coordinates.origin).toMap // start at origin

    for(i <- 0 to iterations) {
      // updates take influence only in next round
      val updateRound = makeMapFuture(
        operators.map(operator => { // run each operator's current round update in parallel
          val dependencyCoordinates = findDependencyCoordinates(operator, operatorDependencyMap.toMap, currentOperatorCoordinates, publisherCoordinates, clientCoordinates)
          operator -> calculateVCSingleOperator(cluster, operator, currentOperatorCoordinates(operator), dependencyCoordinates, nnCandidateCoordinates)
        }).toMap)
      updateRound.onComplete {
        case Success(value) =>
        case Failure(exception) => SpecialStats.debug(s"$this", s"failed in initial iteration $i, \n cause: ${exception} \n ${exception.getStackTrace.map("\n" + _)}")
      }

      val updatedCoordinates = Await.result(updateRound, resolveTimeout.duration) // block here to wait for all operators to finish their update round
      currentOperatorCoordinates = updatedCoordinates
      SpecialStats.debug(s"$this", s"completed initial iteration $i")
    }
    currentOperatorCoordinates
  }

  /**
    * called once the virtual coordinates have been determined to place an operator on a host
    * only used by algorithms that have a separate initial placement logic
    * @param virtualCoordinates
    * @param candidates
    * @param operator
    * @param parents
    * @return hostInfo including the hosting Member, its bdp to the parent nodes, and the msgOverhead of the placement
    */
  def findHost(virtualCoordinates: Coordinates, candidates: Map[Member, Coordinates], operator: Query, parents: List[ActorRef]): Future[HostInfo] = {
    if(!hasInitialPlacementRoutine()) throw new IllegalAccessException("only algorithms with a non-sequential initial placement should call findHost() !")

    for {
      host <- selectHostFromCandidates(virtualCoordinates, candidates, Some(operator))
      bdpUpdate <- this.updateOperatorToParentBDP(cluster, operator, host, parents)
    } yield {
      val hostInfo = HostInfo(host, operator, this.getPlacementMetrics(operator))
      memberLoads.remove(host) // get new value with deployed operator on next request
      placementMetrics.remove(operator) // placement of operator complete, clear entry
      SpecialStats.debug(name, s"found host: $host")
      hostInfo
    }
  }

  def selectHostFromCandidates(coordinates: Coordinates, memberToCoordinates: Map[Member, Coordinates], operator: Option[Query] = None): Future[Member]

  def initialize(cluster: Cluster, test: Boolean = false, publisherNameMap: Option[Map[String, ActorRef]] = None, caller: Option[ActorRef] = None): Future[Boolean] = {
    if(initialized){
      Future { SpecialStats.log(s"${this.toString} $caller", "placementInit", s"placementStrategy is already initialized, skipping initialization"); true }
    } else synchronized {
      this.cluster = cluster
      this.placementMetrics.clear()
      this.initialized_on_node = s"${cluster.selfMember.toString} ${this.getClass} ${SpecialStats.getTimestamp}"
      SpecialStats.log(s"${this.toString} $caller", "placementInit", s" placementStrategy was not yet initialized, fetching publisherActor list and bandwidths from local taskManager")
      for {
        publisherMap <- TCEPUtils.getPublisherActors(cluster)
        bandwidthMap <- TCEPUtils.getAllBandwidthsFromLocalTaskManager(cluster)
      } yield {
        SpecialStats.log(s"${this.toString} $caller", "placementInit", s" bw and pub requests complete (${bandwidthMap.size} bws, ${publisherMap.size} pubs")
        this.publishers = publisherMap
        SpecialStats.log(s"${this.toString} $caller", "placementInit", s" assigned publisher map")
        this.memberBandwidths = bandwidthMap
        SpecialStats.log(s"${this.toString} $caller", "placementInit", s" now ${publishers.size} publisher hosts ${publishers.keys}")
        SpecialStats.log(s"${this.toString} $caller", "placementInit", s" now ${memberBandwidths.size} bandwidth measurements from local taskManager, initialization complete")
        //if (!test) cluster.system.scheduler.schedule(new FiniteDuration(0, TimeUnit.SECONDS), new FiniteDuration(refreshInterval, TimeUnit.SECONDS))(refreshTask) // TODO cause?
        initialized = true
        SpecialStats.log(s"${this.toString} $caller", "placementInit", s" algorithm initialization complete \n publishers: \n ${publishers.keys.mkString("\n")}")
        false
      }

    }
  }

  def refreshTask: Unit = {
    /*
    bwCounter += 1
    if(bwCounter % 6 == 0) {
      memberBandwidths.clear()
      memberLoads.clear()
    } // do not clear bandwidth and load cache every 10s, but every minute
    */
    updateCoordinateMap()
  }

  def updateCoordinateMap(): Future[Map[Member, Coordinates]] = {
    makeMapFuture(cluster.state.members.filter(m=> m.status == MemberStatus.up && !m.hasRole("VivaldiRef"))
      .map(m => {
        val request = TCEPUtils.getCoordinatesOfNode(cluster, m.address).map { result => memberCoordinates = memberCoordinates.updated(m, result); result }
        //request.onComplete {
        //  case Success(value) => log.info( s"retrieved coordinates $value for $m")
        //  case Failure(exception) => log.info( s"failed to retrieve coords for $m \n $exception")
        m -> request
      }).toMap)
    //log.info(s"member coordinates: \n ${memberCoordinates.map(m => s"\n ${m._1.address} | ${m._2} " )} \n")
  }

  protected def updateOperatorToParentBDP(cluster: Cluster, operator: Query, host: Member, parents: List[ActorRef]): Future[Map[String, Double]] = {
    SpecialStats.log(this.toString, "placement", s"starting bdp calculation to parents for operator $operator")
    val bdpToParents: Future[Map[String, Double]] = for {
      opToParentBDP <- makeMapFuture(parents.map(p => {
                            val bdp = for {
                              hostCoords <- getCoordinatesOfNode(cluster, host, None)
                              parentCoords <- getCoordinatesOfNode(cluster, p, None)
                              bw <- this.getBandwidthBetweenMembers(host, TCEPUtils.getMemberFromActorRef(cluster, p), None)
                            } yield parentCoords.distance(hostCoords) * bw
                            p.toString -> bdp // use omit operator since message overhead from this is not placement-related
                            }).toMap)
    } yield {
      val operatorMetrics = placementMetrics.getOrElse(operator, OperatorMetrics())
      operatorMetrics.operatorToParentBDP = opToParentBDP
      placementMetrics += operator -> operatorMetrics
      SpecialStats.log(s"$this", "placement", s"finished bdp calculation from new host ${host.address} to parents ${parents.map(_.path)}; for operator $operator; result: $opToParentBDP")
      opToParentBDP
    }

    bdpToParents.onComplete {
      case Success(value) => log.info(s"operator bdp between (${value}) and host $host of ${operator.getClass}")
      case Failure(exception) => log.error(s"failed to update bdp between host $host and parents ${parents} of ${operator} \n cause $exception")
    }
    bdpToParents
  }

  /**
    * called by any method that incurs communication with other nodes in the process of placing an operator
    * updates an operator's placement msgOverhead (amount of bytes sent as messages to other nodes)
    * accMsgOverhead: accumulated overhead from all directly placement-related communication that has been made for placing this operator
    * @param operator
    */
  protected def updateOperatorMsgOverhead(operator: Option[Query], msgOverhead: Long): Unit = {
    if(operator.isDefined) {
      val operatorMetrics = placementMetrics.getOrElse(operator.get, OperatorMetrics())
      operatorMetrics.accPlacementMsgOverhead += msgOverhead
      placementMetrics.update(operator.get, operatorMetrics)
    }
  }

  protected def getPlacementMetrics(operator: Query): OperatorMetrics = placementMetrics.getOrElse(operator, {
    log.warn(s"could not find placement metrics for operator $operator, returning zero values!")
    OperatorMetrics()
  })

  /**
    * find the n nodes closest to this one
    *
    * @param n       n closest
    * @param candidates map of all neighbour nodes to consider and their vivaldi coordinates
    * @return the nodes closest to this one
    */
  def getNClosestNeighboursByMember(n: Int, candidates: Map[Member, Coordinates] = memberCoordinates): Seq[(Member, Coordinates)] =
    getNClosestNeighboursToCoordinatesByMember(n, DistVivaldiActor.localPos.coordinates, candidates)

  def getClosestNeighbourToCoordinates(coordinates: Coordinates, candidates: Map[Member, Coordinates] = memberCoordinates): (Member, Coordinates) =
    getNClosestNeighboursToCoordinatesByMember(1, coordinates, candidates).head

  def getNClosestNeighboursToCoordinatesByMember(n: Int, coordinates: Coordinates, candidates: Map[Member, Coordinates] = memberCoordinates): Seq[(Member, Coordinates)] =
    getNeighboursSortedByDistanceToCoordinates(candidates, coordinates).take(n)

  def getNeighboursSortedByDistanceToCoordinates(neighbours: Map[Member, Coordinates], coordinates: Coordinates): Seq[(Member, Coordinates)] =
    if(neighbours.nonEmpty) {
      val sorted = neighbours.map(m => m -> m._2.distance(coordinates)).toSeq.sortWith(_._2 < _._2)
      val keys = sorted.map(_._1)
      keys

    } else {
      log.debug("getNeighboursSortedByDistanceToCoordinates() - received empty coordinate map, returning self")
      Seq(cluster.selfMember -> DistVivaldiActor.localPos.coordinates)
    }

  protected def getBandwidthBetweenCoordinates(c1: Coordinates, c2: Coordinates, nnCandidates: Map[Member, Coordinates], operator: Option[Query] = None): Future[Double] = {
    val source: Member = getClosestNeighbourToCoordinates(c1, nnCandidates)._1
    val target: Member = getClosestNeighbourToCoordinates(c2, nnCandidates.filter(!_._1.equals(source)))._1
    //SpecialStats.debug(s"$this", s"retrieving bandwidth between $source and $target")
    val request = getBandwidthBetweenMembers(source, target, operator)
    request
  }

  /**
    * get the bandwidth between source and target Member; use cached values if available to avoid measurement overhead from iperf
    * @param source
    * @param target
    * @return
    */
  protected def getBandwidthBetweenMembers(source: Member, target: Member, operator: Option[Query]): Future[Double] = {
    if(source.equals(target)) Future { 0.0d }
    else if(memberBandwidths.contains((source, target))) {
      Future { memberBandwidths.getOrElse((source, target), defaultBandwidth) }
    } else if(memberBandwidths.contains((target, source))) {
      Future { memberBandwidths.getOrElse((target, source), defaultBandwidth) }
    } else {
      val communicationOverhead = SizeEstimator.estimate(SingleBandwidthRequest(cluster.selfMember)) + SizeEstimator.estimate(SingleBandwidthResponse(0.0d))
      this.updateOperatorMsgOverhead(operator, communicationOverhead)
      val request = for {
        bw <- TCEPUtils.getBandwidthBetweenNodes(cluster, source, target)
      } yield {
        memberBandwidths += (source, target) -> bw
        memberBandwidths += (target, source) -> bw
        bw
      }
      request.onComplete {
        case Success(bw) => SpecialStats.debug(s"$this", s"retrieved bandwidth $bw between $source and $target, caching it locally")
        case Failure(exception) => SpecialStats.debug(s"$this", s"failed to retrieve bandwidth between $source and $target, cause: $exception")
      }
      request
    }
  }

  protected def getAllBandwidths(cluster: Cluster, operator: Option[Query]): Future[Map[(Member, Member), Double]] = {
    SpecialStats.log(this.name, "placement", s"sending AllBandwidthsRequest to local taskManager")
    for {
      bandwidths <- TCEPUtils.getAllBandwidthsFromLocalTaskManager(cluster).mapTo[Map[(Member, Member), Double]]
    } yield {
      SpecialStats.log(this.name, "placement", s"received ${bandwidths.size} measurements")
      val communicationOverhead = SizeEstimator.estimate(AllBandwidthsRequest()) + SizeEstimator.estimate(AllBandwidthsResponse(bandwidths))
      this.updateOperatorMsgOverhead(operator, communicationOverhead)
      memberBandwidths ++= bandwidths
      bandwidths
    }
  }

  def getMemberByAddress(address: Address): Option[Member] = cluster.state.members.find(m => m.address.equals(address))

  /**
    * retrieve all nodes that can host operators, including !publishers!
    *
    * @param cluster cluster reference to retrieve nodes from
    * @return all cluster members (nodes) that are candidates (EmptyApp) or publishers (PublisherApp)
    */
  def findPossibleNodesToDeploy(cluster: Cluster): Set[Member] = cluster.state.members.filter(x =>
    // MemberStatus.Up is still true if node was marked temporarily unreachable!
    x.status == MemberStatus.Up && x.hasRole("Candidate") && !cluster.state.unreachable.contains(x))

  /**
    * retrieves the vivaldi coordinates of a node from its actorRef (or Member)
    * attempts to contact the node 3 times before returning default coordinates (origin)
    * records localMsgOverhead from communication for placement metrics
    * @param node the actorRef of the node
    * @param operator the operator with which the incurred communication overhead will be associated
    * @return its vivaldi coordinates
    */
  def getCoordinatesOfNode(cluster: Cluster, node: ActorRef,  operator: Option[Query]): Future[Coordinates] = this.getCoordinatesFromAddress(cluster, node.path.address, operator)
  def getCoordinatesOfNode(cluster: Cluster, node: Member, operator: Option[Query]): Future[Coordinates] = this.getCoordinatesFromAddress(cluster, node.address, operator)
  // blocking, !only use in tests!
  def getCoordinatesOfNodeBlocking(cluster: Cluster, node: Member, operator: Option[Query] = None): Coordinates = Await.result(this.getCoordinatesFromAddress(cluster, node.address, operator), requestTimeout)

  protected def getCoordinatesFromAddress(cluster: Cluster, address: Address, operator: Option[Query] = None, attempt: Int = 0): Future[Coordinates] = {

    val maxTries = 3
    val member = getMemberByAddress(address)
    if (member.isDefined && memberCoordinates.contains(member.get)) Future { memberCoordinates(member.get) }
    else {
      this.updateOperatorMsgOverhead(operator, coordRequestSize)
      val request: Future[Coordinates] = TCEPUtils.getCoordinatesOfNode(cluster, address)
      request.recoverWith { // retries up to maxTries times if futures does not complete
        case e: Throwable =>
          if (attempt < maxTries) {
            log.warn(s"failed $attempt times to retrieve coordinates of ${member.get}, retrying... \n cause: ${e.toString}")
            this.getCoordinatesFromAddress(cluster, address, operator, attempt + 1)
          } else {
            log.warn(s"failed $attempt times to retrieve coordinates of ${member.get}, returning origin coordinates \n cause: ${e.toString}")
            Future { Coordinates.origin }
          }
      } map { result => {
        this.updateOperatorMsgOverhead(operator, SizeEstimator.estimate(CoordinatesResponse(result)))
        result
      }}
    }
  }

  def getCoordinatesOfMembers(cluster: Cluster, nodes: Set[Member], operator: Option[Query] = None): Future[Map[Member, Coordinates]] = {
    makeMapFuture(nodes.map(node => {
      node -> this.getCoordinatesOfNode(cluster, node, operator)
    }).toMap)
  }
  // callers  have to handle futures
  protected def getCoordinatesOfNodes(cluster: Cluster, nodes: Seq[ActorRef], operator: Option[Query] = None): Future[Map[ActorRef, Coordinates]] =
    makeMapFuture(nodes.map(node => node -> this.getCoordinatesOfNode(cluster, node, operator)).toMap)

  protected def findMachineLoad(cluster: Cluster, nodes: Seq[Member], operator: Option[Query] = None): Future[Map[Member, Double]] = {
    makeMapFuture(nodes.map(n => n -> this.getLoadOfNode(cluster, n, operator).recover {
      case e: Throwable =>
        log.info(s"failed to get load of $n using default load $defaultLoad, cause \n $e")
        defaultLoad
    }).toMap)
  }

  /**
    * retrieves current system load of the node; caches the value if not existing
    * (cache entries are cleared periodically (refreshTask) or if an operator is deployed on that node
    * records localMsgOverhead from communication for placement metrics
    * @param cluster
    * @param node
    * @return
    */
  def getLoadOfNode(cluster: Cluster, node: Member, operator: Option[Query] = None): Future[Double] = {

    if(memberLoads.contains(node)) Future { memberLoads(node) }
    else {
      val request: Future[Double] = TCEPUtils.getLoadOfMember(cluster, node)
      this.updateOperatorMsgOverhead(operator, SizeEstimator.estimate(LoadRequest()) + SizeEstimator.estimate(LoadResponse(_)))
      request.onComplete {
        case Success(load: Double) =>
          memberLoads += node -> load
        case Failure(exception) =>
      }
      request
    }
  }

  protected def getBDPBetweenCoordinates(cluster: Cluster, sourceCoords: Coordinates, targetCoords: Coordinates, candidates: Map[Member, Coordinates] ): Future[Double] = {

    val latency = sourceCoords.distance(targetCoords)
    if (latency == 0.0d) return Future { 0.0d }
    for {
      bw <- this.getBandwidthBetweenCoordinates(sourceCoords, targetCoords, candidates)
    } yield  {
      if(latency <= 0.0d || bw <= 0.0d) log.warn(s"BDP between non-equal nodes $sourceCoords and $targetCoords is zero!")
      latency * bw
    }
  }

  /**
    * recursively extracts the operators and their dependent child and parent operators
    * @param query current operator
    * @param child parent operator
    * @param acc accumulated operators and dependencies
    * @return the sorted map of all operators and their dependencies, with the root operator as the first map entry
    */
  def extractOperators(query: Query, child: Option[Query] = None, acc: mutable.LinkedHashMap[Query, QueryDependencies] = mutable.LinkedHashMap()): mutable.LinkedHashMap[Query, QueryDependencies] = {

    query match {
      case b: BinaryQuery => {
        val left = extractOperators(b.sq1, Some(b), mutable.LinkedHashMap(b -> QueryDependencies(Some(List(b.sq1, b.sq2)), child)))
        val right = extractOperators(b.sq2, Some(b), mutable.LinkedHashMap(b -> QueryDependencies(Some(List(b.sq1, b.sq2)), child)))
        left.foreach(l => acc += l)
        right.foreach(r => acc += r)
        acc
      }
      case u: UnaryQuery => extractOperators(u.sq, Some(u), acc += u -> QueryDependencies(Some(List(u.sq)), child))
      case l: LeafQuery => acc += l -> QueryDependencies(None, child) // stream nodes have no parent operators, they depend on publishers
      case _ =>
        throw new RuntimeException(s"unknown query type $query")
    }
  }

  /**
    * returns a list of coordinates on which the operator is dependent, i.e. the coordinates of its parents and children
    * for stream operators the publisher acts as a parent, for the root operator the client node acts as the child
    * @param operator
    * @param operatorDependencyMap
    * @param currentOperatorCoordinates
    * @param publisherCoordinates
    * @param clientCoordinates
    * @return
    */
  def findDependencyCoordinates(operator: Query, operatorDependencyMap: Map[Query, QueryDependencies], currentOperatorCoordinates: Map[Query, Coordinates],
                                publisherCoordinates: Map[String, Coordinates], clientCoordinates: Coordinates): List[Coordinates] = {


      var dependencyCoordinates = List[Coordinates]()
      operator match {
        case s: StreamQuery =>
          val publisherKey = publisherCoordinates.keys.find(_.contains(s.publisherName.substring(2))) // name without "P:"
          if (publisherKey.isDefined)
            dependencyCoordinates = publisherCoordinates(publisherKey.get) :: dependencyCoordinates // stream operator, set publisher coordinates
          else throw new RuntimeException(s"could not find publisher ${s.publisherName} of stream operator among publisher coordinate map \n ${publisherCoordinates.mkString("\n")}")
          val child = operatorDependencyMap(operator).child
          if (child.isDefined) dependencyCoordinates = currentOperatorCoordinates(child.get) :: dependencyCoordinates
          else {
            //throw new IllegalArgumentException(s"stream operator without child found! $operator"))) :: dependencyCoordinates
            log.warn("found stream operator with no child: this is only okay if the query has only this one operator")
            dependencyCoordinates = clientCoordinates :: dependencyCoordinates
          }

        case _ => // non-stream query
          val dependencies = operatorDependencyMap(operator)
          if (dependencies.child.isEmpty) { // root operator
            dependencyCoordinates = clientCoordinates :: dependencyCoordinates
          } else {
            dependencyCoordinates = currentOperatorCoordinates(dependencies.child.get) :: dependencyCoordinates
          }
          dependencyCoordinates = dependencyCoordinates ++ dependencies.parents
            .getOrElse(throw new IllegalArgumentException(s"non-stream operator without parents found! $operator"))
            .map(parent => currentOperatorCoordinates(parent))

      }
      dependencyCoordinates
  }

}

object PlacementStrategy {

  def getStrategyByName(name: String): PlacementStrategy = {
    name match {
      case PietzuchAlgorithm.name => PietzuchAlgorithm
      case RizouAlgorithm.name => RizouAlgorithm
      case StarksAlgorithm.name => StarksAlgorithm
      case RandomAlgorithm.name => RandomAlgorithm
      case MobilityTolerantAlgorithm.name => MobilityTolerantAlgorithm
      case GlobalOptimalBDPAlgorithm.name => GlobalOptimalBDPAlgorithm
      case other: String => throw new NoSuchElementException(s"need to add algorithm type $other to updateTask!")
    }
  }
}
case class HostInfo(member: Member, operator: Query, var operatorMetrics: OperatorMetrics = OperatorMetrics(), var visitedMembers: List[Member] = List(), var graphTotalBDP: Double = 0)
// accMsgOverhead is placement messaging overhead for all operators from stream to current operator; root operator has placement overhead of entire query graph
case class OperatorMetrics(var operatorToParentBDP: Map[String, Double] = Map(), var accPlacementMsgOverhead: Long = 0)
case class QueryDependencies(parents: Option[List[Query]], child: Option[Query])