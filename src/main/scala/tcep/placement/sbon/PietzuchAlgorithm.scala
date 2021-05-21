package tcep.placement.sbon



import akka.actor.ActorContext
import akka.cluster.{Cluster, Member}
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.placement._
import tcep.utils.SpecialStats
import tcep.utils.TCEPUtils.makeMapFuture

import scala.concurrent.Future

/*
* Each placement mechanism trait extends the PlacementStrategy interface that provides standard extensions
* This trait describes the Relaxation algorithm described in the work of Pietzuch et al. 
* https://ieeexplore.ieee.org/document/1617417
*/
object PietzuchAlgorithm extends PlacementStrategy {

  val stepAdjustmentEnabled: Boolean = ConfigFactory.load().getBoolean("constants.placement.relaxation-step-adjustment-enabled")
  val iterationLimit = ConfigFactory.load().getInt("constants.placement.max-single-operator-iterations")
  val defaultBandwidth = ConfigFactory.load().getDouble("constants.default-data-rate")
  var stepSize = ConfigFactory.load().getDouble("constants.placement.relaxation-initial-step-size")
  var k = ConfigFactory.load().getInt("constants.placement.physical-placement-nearest-neighbours")
  var nodeLoads = Map[Member, Double]()
  override val name = "Relaxation"
  override def hasInitialPlacementRoutine(): Boolean = true
  override def hasPeriodicUpdate(): Boolean = ConfigFactory.load().getBoolean("constants.placement.update.relaxation")

  // this should only be called during transitions or in case of missing operators during the initial placement
  def findOptimalNode(context: ActorContext, cluster: Cluster, dependencies: Dependencies, askerInfo: HostInfo, operator: Query): Future[HostInfo] = {
    applyRelaxationAlgorithm(cluster, dependencies, operator)
  }

  def findOptimalNodes(context: ActorContext, cluster: Cluster, dependencies: Dependencies, askerInfo: HostInfo, operator: Query): Future[(HostInfo, HostInfo)] = {
    for {
      mainNode <- applyRelaxationAlgorithm(cluster, dependencies, operator)
    } yield {
      val backup = HostInfo(cluster.selfMember, operator, OperatorMetrics())
      (mainNode, backup)
    }
  }

  /**
    * Applies Pietzuch's algorithm to find the optimal node for a single operator
    * https://ieeexplore.ieee.org/document/1617417
    *
    * @param dependencies   parent nodes on which query is dependent (parent and children operators; caller is responsible for completeness!)
    * @return the address of member where operator will be deployed
    */

  def applyRelaxationAlgorithm(cluster: Cluster, dependencies: Dependencies, operator: Query): Future[HostInfo] = {

    val res = for {
      init <- this.initialize(cluster)
    } yield {

      val candidateCoordRequest = getCoordinatesOfMembers(cluster, findPossibleNodesToDeploy(cluster), Some(operator))
      val dependencyCoordRequest = getCoordinatesOfNodes(cluster, dependencies.parents ++ dependencies.subscribers, Some(operator))
      val hostRequest = for {
        candidateCoordinates <- candidateCoordRequest
        dependencyCoordinates <- dependencyCoordRequest
        optimalVirtualCoordinates <- calculateVCSingleOperator(cluster, operator, Coordinates.origin, dependencyCoordinates.values.toList, candidateCoordinates)
        host <- findHost(optimalVirtualCoordinates, candidateCoordinates, operator, dependencies.parents)
      } yield {

        log.info(s"found node to host operator: ${host.toString}")
        SpecialStats.debug(name, s"found node: ${host.member}")
        val hostCoords = candidateCoordinates.find(e => e._1.equals(host.member))
        if (hostCoords.isDefined) log.info(s"with distance to virtual coords: ${hostCoords.get._2.distance(optimalVirtualCoordinates)}")
        else log.warn("found node, but coordinatesMap does not contain its coords")
        host
      }
      hostRequest
    }
    res.flatten
  }

  /**
    * determine the optimal position of a single operator in the virtual vivaldi coordinate space
    * uses a spring-relaxation technique:
    * edges on the operator tree are modeled as springs; the length of a spring corresponds to the latency between the operators on its ends,
    * the spring constant corresponds to the bandwidth on that link. Now the goal is to solve for the minimum energy state in all springs
    * by iteratively moving operators in the latency space in small steps into the direction of the force exerted by the springs connected to them.
    *
    * @param dependencyCoordinates the coordinates of all operator hosts that are connected to this operator (child and parents)
    * @param nnCandidates cluster members that can host operators (used for bandwidth estimation)
    * @return the optimal vivaldi coordinates for the operator
    */
  def calculateVCSingleOperator(cluster: Cluster, operator: Query, startCoordinates: Coordinates, dependencyCoordinates: List[Coordinates], nnCandidates: Map[Member, Coordinates]): Future[Coordinates] = {

    // check if there is any significant distance between the dependency coordinates (> 0.001)
    if(Coordinates.areAllEqual(dependencyCoordinates)) {
      Future { dependencyCoordinates.head }
    } else {
      this.cluster = cluster
      val previousRoundBDP = Double.MaxValue
      val startCoordinates = Coordinates.origin
      val resultVC = for {
        bwToDependenciesRequest <- makeMapFuture(dependencyCoordinates.map(dependency => dependency -> getBandwidthBetweenCoordinates(dependency, startCoordinates, nnCandidates, Some(operator))).toMap)
        bandwidthsToDependencies = bwToDependenciesRequest.map(dependency => dependency._1 -> math.max(dependency._2, defaultBandwidth) / 1000.0d)
      } yield makeVCStep(startCoordinates, stepSize, previousRoundBDP, dependencyCoordinates, nnCandidates, bandwidthsToDependencies)
      resultVC.flatten
    }
  }

  def makeVCStep(vc: Coordinates, stepSize: Double, previousRoundForce: Double,
                 dependencyCoordinates: List[Coordinates], nnCandidates: Map[Member, Coordinates],
                 currentVCToDependenciesBandwidths: Map[Coordinates, Double],
                 iteration: Int = 0): Future[Coordinates] = {

    //SpecialStats.debug(s"$this", s"virtual coordinate iteration $iteration start")

    // Relaxation minimizes SUM( datarate(p, v) * latency(p, v)^2 )
    // reason according to paper: 'additional squared exponent in the function ensures that there is a unique solution from a set of placements with equal network usage'
    // since distances between coordinates represent latencies; the length of the vector from A to B is the latency between A and B
    // -> we need to square the length of the vector between A and B
    // -> multiply the vector by its length; the scaled vectors length is the square of the original length (latency)
    // | x * |x| |  = |x|^2
    // look up the node closest to the current virtual coordinates, use the bandwidth between this node and the dependency as an estimate for bw between the coordinate pair
    //  what happens if nearest node is the dependency? -> for now use two closest neighbours
    // coord distance in ms, data rate in Mbit/s
    var maxDist = 0.0d // longest observed latency
    val dependencyForces = currentVCToDependenciesBandwidths.map(dependency => {
      // f = f + datarate(p , v) * (p - v)²
      val vectorToDep = dependency._1.sub(vc)
      if(vectorToDep.measure() >= maxDist) maxDist = vectorToDep.measure()
      val squaredVectorToDep = vectorToDep.scale(vectorToDep.measure()) // squared latency
      val dependencyIncrement: Coordinates = squaredVectorToDep.scale(dependency._2) // latency² * bandwidth = BDP'
      dependencyIncrement
    }).toList

    val forcesTotal: Coordinates = dependencyForces.foldLeft(Coordinates(0, 0, 0))(_.add(_))
    val currentRoundForce = forcesTotal.measure()
    // use unity vector scaled by step size here to not make ludicrously large steps; relative influence of data rates on f is preserved
    var step = Coordinates.origin
    if(stepAdjustmentEnabled) step = forcesTotal.unity().scale(stepSize * maxDist)
    else step = forcesTotal.scale(stepSize)
    val updatedVC = vc.add(step)
    var updatedStepSize = stepSize
    //log.debug(s"\ncalculateVCSingleOperator() iteration $iteration - " +
      //s"\nprevious round force size: ${previousRoundForce}" +
      //s"\nstep vector: ${step.toString} step size: ${step.measure()}" +
      //s"\nvirtualCoords updated: ${updatedVC.toString} \n")
    // adjust stepSize to make steps smaller when force vector starts growing between two iterations
    // (we want the force vector's length to approach zero)
    if(stepAdjustmentEnabled) { // paper did not feature step size adjustment, but algorithm does not converge well/at all without it
      if (forcesTotal.measure() > previousRoundForce) {
        updatedStepSize = stepSize * 0.5d
      } else {
        updatedStepSize = stepSize * 1.1d
      }
    }

    val result = for {
      // look up the node closest to the current virtual coordinates, use the bandwidth between this node and the dependency as an estimate for bw between the coordinate pair
      // what happens if nearest node is the dependency? -> use two closest neighbours
      bwToDependenciesRequest <- makeMapFuture(dependencyCoordinates.map(dependency => dependency -> this.getBandwidthBetweenCoordinates(dependency, updatedVC, nnCandidates)).toMap)
      updatedVCToDependenciesBandwidths = bwToDependenciesRequest.map(dependency => dependency._1 -> math.max(dependency._2, defaultBandwidth) / 1000.0d) // coord distance in ms, data rate in Mbit/s
    } yield {
      val forceThreshold = 0.1d
      if(currentRoundForce >= forceThreshold && iteration < iterationLimit) {
        makeVCStep(updatedVC, updatedStepSize, forcesTotal.measure(), dependencyCoordinates, nnCandidates, updatedVCToDependenciesBandwidths, iteration + 1)

      } else { // terminate
        if(iteration >= iterationLimit) log.warn(s"maximum iteration limit ($iterationLimit) reached, returning virtual coordinates $updatedVC now!")
        //log.debug(s"\n calculateVCSingleOperator() iteration $iteration - " +
          //s"\n previous round force: $previousRoundForce; current round force ${currentRoundForce} ; delta ${math.abs(currentRoundForce - previousRoundForce)}, threshold: $forceThreshold" +
          //s"\n step vector: ${step.x} ${step.y} step size: $stepSize" +
          //s"\n virtualCoords: $vc, virtualCoords updated: ${updatedVC} \n")

        Future { updatedVC }
      }
    }
    result.flatten
  }

  /**
    * looks for the k members closest to the virtual coordinates and returns the one with minimum load
    * @param virtualCoordinates
    * @param candidates members to be considered
    * @return member with minimum load
    */
  // TODO once operator re-use is implemented, this must be adapted according to the paper
  def selectHostFromCandidates(virtualCoordinates: Coordinates, candidates: Map[Member, Coordinates], operator: Option[Query] = None): Future[Member] = {
    val host = for {
      candidatesMap <- if(candidates == null) getCoordinatesOfMembers(cluster, findPossibleNodesToDeploy(cluster), operator)
      else Future { candidates }
      machineLoads: Map[Member, Double] <- findMachineLoad(cluster, getNClosestNeighboursToCoordinatesByMember(k, virtualCoordinates, candidatesMap).map(_._1), operator)
    } yield {
      val host = machineLoads.toList.minBy(_._2)._1
      host
    }
    host
  }

}
