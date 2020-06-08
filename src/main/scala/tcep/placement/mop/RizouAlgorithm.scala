package tcep.placement.mop

import akka.actor.ActorContext
import akka.cluster.{Cluster, Member}
import akka.japi.Option.Some
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.placement._
import tcep.utils.SpecialStats
import tcep.utils.TCEPUtils.makeMapFuture

import scala.concurrent.Future


/**
  * Implementation of MOP algorithm introduced by Stamatia Rizou et. al.
  * http://ieeexplore.ieee.org/abstract/document/5560127/
  * Note: assumes that there is only one consumer for a graph, i.e. operators must not be shared (re-used) between graphs!
  */

object RizouAlgorithm extends PlacementStrategy {

  var k = ConfigFactory.load().getInt("constants.placement.physical-placement-nearest-neighbours")
  val loadThreshold = ConfigFactory.load().getDouble("constants.placement.physical-placement-node-overload-threshold")
  val iterationLimit = ConfigFactory.load().getInt("constants.placement.max-single-operator-iterations")
  val defaultBandwidth = ConfigFactory.load().getDouble("constants.default-data-rate")
  val minimumImprovementThreshold = 0.001d // minimum improvement of network usage (Mbit) per round
  override val name = "Rizou"
  override def hasInitialPlacementRoutine(): Boolean = true
  override def hasPeriodicUpdate(): Boolean = ConfigFactory.load().getBoolean("constants.placement.update.rizou")

  // this should only be called during transitions or in case of missing operators during the initial placement
  def findOptimalNode(context: ActorContext, cluster: Cluster, dependencies: Dependencies, askerInfo: HostInfo, operator: Query): Future[HostInfo] = {
    applyRizouAlgorithm(cluster, dependencies, operator)
  }

  def findOptimalNodes(context: ActorContext, cluster: Cluster, dependencies: Dependencies, askerInfo: HostInfo, operator: Query) =
    for {
      mainNode <- applyRizouAlgorithm(cluster, dependencies, operator)
    } yield {
      val backup = HostInfo(cluster.selfMember, operator, OperatorMetrics())
      (mainNode, backup)
    }

  /**
    * Applies Rizou's algorithm to find the optimal node for a single operator
    * @param dependencies   parent nodes on which query is dependent (parent and children operators; caller is responsible for completeness!)
    * @return the address of member where operator will be deployed
    */

  def applyRizouAlgorithm(cluster: Cluster, dependencies: Dependencies, operator: Query): Future[HostInfo] = {

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
        SpecialStats.debug(this.getClass.getSimpleName, s"placing $operator on host: ${host.member}")
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
    if(Coordinates.areAllEqual(startCoordinates :: dependencyCoordinates)) {
      Future { startCoordinates }
    } else {
      this.cluster = cluster
      val stepSize: Double = dependencyCoordinates.map(c => c.distance(startCoordinates)).max / 2
      val previousRoundBDP = Double.MaxValue
      val resultVC = for {
        bwToDependenciesRequest <- makeMapFuture(dependencyCoordinates.map(dependency => dependency -> getBandwidthBetweenCoordinates(dependency, startCoordinates, nnCandidates, Some(operator))).toMap)
        bandwidthsToDependencies = bwToDependenciesRequest.map(dependency => dependency._1 -> math.max(dependency._2, defaultBandwidth) / 1000.0d)
      } yield makeVCStep(operator = operator, stepSize, previousRoundBDP, dependencyCoordinates, nnCandidates, bandwidthsToDependencies, vc = startCoordinates)
      resultVC.flatten
    }
  }

  def makeVCStep(operator: Query, stepSize: Double, previousRoundBDP: Double, dependencyCoordinates: List[Coordinates], nnCandidates: Map[Member, Coordinates], currentVCToDependenciesBandwidths: Map[Coordinates, Double], iteration: Int = 0, vc: Coordinates): Future[Coordinates] = {

    // Rizou minimizes SUM( datarate(p, v) * latency(p, v)
    val dependencyForces = currentVCToDependenciesBandwidths.map(dependency => {
      // f = f + datarate(p , v) * (p - v)
      val vectorToDep = dependency._1.sub(vc)
      val dependencyIncrement: Coordinates = vectorToDep.scale(dependency._2) // latency * bandwidth = BDP
      dependencyIncrement
    }).toList

    val forcesTotal: Coordinates = dependencyForces.foldLeft(Coordinates(0, 0, 0))(_.add(_))
    // use unity vector scaled by step size here to not make ludicrously large steps; relative influence of data rates on f is preserved
    val step = forcesTotal.unity().scale(stepSize)
    val updatedVC = vc.add(step)

    val result = for {
      // look up the node closest to the current virtual coordinates, use the bandwidth between this node and the dependency as an estimate for bw between the coordinate pair
      // what happens if nearest node is the dependency? -> use two closest neighbours
      bwToDependenciesRequest <- makeMapFuture(dependencyCoordinates.map(dependency => dependency -> this.getBandwidthBetweenCoordinates(dependency, vc.add(step), nnCandidates, Some(operator))).toMap)
      updatedVCToDependenciesBandwidths = bwToDependenciesRequest.map(dependency => dependency._1 -> math.max(dependency._2, defaultBandwidth) / 1000.0d) // coord distance in ms, data rate in Mbit/s
      bdpsToDependencies = updatedVCToDependenciesBandwidths.map(dependency => dependency._1 -> dependency._1.distance(updatedVC) * dependency._2) // ms * Mbit/ms = Mbit
    } yield {
      val currentRoundBDP = bdpsToDependencies.values.sum
      val delta: Double = math.abs(currentRoundBDP - previousRoundBDP) // abs is missing in paper, but without it we would terminate after one step

      if(delta >= minimumImprovementThreshold && iteration < iterationLimit) {
        // adjust stepSize to make steps smaller when network usage stops shrinking between two iterations
        if(currentRoundBDP > previousRoundBDP) {
          val updatedStepSize = stepSize / 2.0 // do not pass updated virtual coordinates here -> try again with smaller step
          makeVCStep(operator, updatedStepSize, previousRoundBDP, dependencyCoordinates, nnCandidates, currentVCToDependenciesBandwidths, iteration + 1, vc)
        } else {
          makeVCStep(operator, stepSize, currentRoundBDP, dependencyCoordinates, nnCandidates, updatedVCToDependenciesBandwidths, iteration + 1, updatedVC)
        }

      } else { // terminate
        if(iteration >= iterationLimit) log.warn(s"maximum iteration limit ($iterationLimit) reached, returning virtual coordinates $vc (last delta: $delta) now!")
        //log.debug(s"\n calculateVCSingleOperator() iteration $iteration - " +
          //s"\n previous round BDP: $previousRoundBDP; current round BDP $currentRoundBDP ; delta $delta, threshold: $minimumImprovementThreshold" +
          //s"\n step vector: ${step.x} ${step.y} step size: $stepSize" +
          //s"\n virtualCoords: $vc, virtualCoords updated: ${vc.add(step).toString} \n")

         Future { vc }
      }
    }
    result.flatten
  }

  /**
    * looks for the k members closest to the virtual coordinates and returns the closest that is not overloaded
    * @param virtualCoordinates
    * @param candidates members to be considered
    * @return member with minimum load
    */
  def selectHostFromCandidates(virtualCoordinates: Coordinates, candidates: Map[Member, Coordinates], operator: Option[Query] = None): Future[Member] = {
    val host = for {
      candidatesMap <- if(candidates == null) getCoordinatesOfMembers(cluster, findPossibleNodesToDeploy(cluster), operator)
      else Future { candidates }
      machineLoads: Map[Member, Double] <- findMachineLoad(cluster, getNClosestNeighboursToCoordinatesByMember(k, virtualCoordinates, candidatesMap).map(_._1), operator)
    } yield {
      val sortedLoads = machineLoads.toList.sortBy(_._2)
      val host = sortedLoads.find(_._2 <= loadThreshold).getOrElse(sortedLoads.head)._1 // chose the closest non-overloaded node
      host
    }
    host
  }

}
