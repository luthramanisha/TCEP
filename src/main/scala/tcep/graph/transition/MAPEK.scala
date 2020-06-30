package tcep.graph.transition

import java.time.{Duration, Instant}

import akka.actor.{ActorContext, ActorRef, PoisonPill}
import akka.pattern.ask
import akka.util.Timeout
import tcep.ClusterActor
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.transition.MAPEK._
import tcep.graph.transition.mapek.lightweight.LightweightMAPEK
import tcep.graph.transition.mapek.requirementBased.RequirementBasedMAPEK
import tcep.machinenodes.helper.actors.TransitionControlMessage
import tcep.placement.PlacementStrategy
import tcep.placement.benchmarking.{BenchmarkingNode, NetworkChurnRate}
import tcep.placement.sbon.PietzuchAlgorithm
import tcep.simulation.tcep._
import tcep.utils.SpecialStats

import scala.concurrent.duration._

trait MAPEKComponent extends ClusterActor {
  implicit val timeout = Timeout(5 seconds)
  override implicit val ec = blockingIoDispatcher // do not use the default dispatcher to avoid starvation at high event rates
}
trait MonitorComponent extends MAPEKComponent
trait AnalyzerComponent extends MAPEKComponent
abstract class PlannerComponent(mapek: MAPEK) extends MAPEKComponent {

  override def receive: Receive = {

    case ManualTransition(algorithmName) =>
      log.info(s"sending manualTransition request for $algorithmName to executor ")
      mapek.executor ! ExecuteTransition(algorithmName)
  }
}

abstract class ExecutorComponent(mapek: MAPEK) extends MAPEKComponent {

  override def preStart() = {
    super.preStart()
    log.info("starting MAPEK Executor")
  }

  override def receive: Receive = {

    case ExecuteTransition(strategyName) =>
      log.info(s"received ExecuteTransition to $strategyName")
      for {
        currentStrategyName <- (mapek.knowledge ? GetPlacementStrategyName).mapTo[String]
        client<- (mapek.knowledge ? GetClient).mapTo[ActorRef]
        mode <- (mapek.knowledge ? GetTransitionMode).mapTo[TransitionConfig]
        status <- (mapek.knowledge ? GetTransitionStatus).mapTo[Int]
      } yield {
        if (currentStrategyName != strategyName && status != 1) {
          val placementStrategy = PlacementStrategy.getStrategyByName(strategyName)
          mapek.knowledge ! SetPlacementStrategy(placementStrategy)
          log.info(s"executing $mode transition to ${placementStrategy.name}")
          client ! TransitionRequest(placementStrategy, self, TransitionStats(0, 0, System.currentTimeMillis()))
        } else log.info(s"received ExecuteTransition message: not executing transition to $strategyName " +
          s"since it is already active or another transition is still in progress (status: $status)")
      }
  }
}

abstract case class KnowledgeComponent(query: Query, var transitionConfig: TransitionConfig, var currentPlacementStrategy: PlacementStrategy, var transitionStatus: Int = 0, var operators: Set[ActorRef] = Set(), var backupOperators: Set[ActorRef] = Set()) extends MAPEKComponent {

  var client: ActorRef = _
  protected val requirements: scala.collection.mutable.Set[Requirement] = scala.collection.mutable.Set(pullRequirements(query, List()).toSeq: _*)
  var deploymentComplete: Boolean = false
  var lastTransitionEnd: Long = System.currentTimeMillis()
  var lastTransitionDuration: Long = 0
  var lastTransitionStats: TransitionStats = TransitionStats()
  var latencyMovingAverage: Double = 0.0
  var previousLatencies: Map[Long, Long] = Map()

  override def receive: Receive = {

    case IsDeploymentComplete => sender() ! this.deploymentComplete
    case SetDeploymentStatus(isComplete) => this.deploymentComplete = isComplete
    case GetPlacementStrategyName => sender() ! currentPlacementStrategy.name
    case SetPlacementStrategy(newStrategy) =>
      this.currentPlacementStrategy = newStrategy
      log.info(s"updated current placementStrategy to $currentPlacementStrategy")
    case GetRequirements => sender() ! this.requirements.toList
    case ar: AddRequirement =>
      ar.requirements.foreach(req => this.requirements += req)
      log.info(s"added requirements ${ar.requirements}")
    case rm: RemoveRequirement =>
      rm.requirements.foreach(req => this.requirements -= req)
      log.info(s"removed requirements ${rm.requirements}")
    case GetTransitionMode => sender() ! transitionConfig
    case SetTransitionMode(mode: TransitionConfig) => this.transitionConfig = mode
    case GetTransitionStatus => sender() ! this.transitionStatus
    case SetTransitionStatus(status: Int) =>
      this.transitionStatus = status
      this.client ! SetTransitionStatus(status)
    case GetLastTransitionStats => sender() ! (this.lastTransitionStats, this.lastTransitionDuration)
    case SetLastTransitionStats(stats) =>
      this.lastTransitionEnd = System.currentTimeMillis()
      this.lastTransitionStats = stats
      this.lastTransitionDuration = lastTransitionEnd - stats.transitionStartAtKnowledge
      val reqname = if(requirements.nonEmpty) requirements.head.name
      SpecialStats.log(this.getClass.toString, s"transitionStats-perQuery-${transitionConfig}-${currentPlacementStrategy.name}}",
        s"total;transition time;${lastTransitionDuration};ms;" +
          s"placementOverheadKBytes;${lastTransitionStats.placementOverheadBytes / 1000.0};" +
          s"transitionOverheadKBytes;${lastTransitionStats.transitionOverheadBytes / 1000.0};" +
          s"combinedOverheadKBytes;${(lastTransitionStats.transitionOverheadBytes + lastTransitionStats.placementOverheadBytes) / 1000.0}")
      //lastTransitionStats.transitionTimesPerOperator.foreach(op =>
      // SpecialStats.log(this.getClass.toString, s"transitionStats-$transitionConfig-${currentPlacementStrategy.name}-${reqname}", s"operator;${op._1};${op._2};"))

    case GetLastTransitionDuration => sender() ! this.lastTransitionDuration
    case GetLastTransitionEnd => sender() ! lastTransitionEnd
    case GetClient => sender() ! this.client
    case SetClient(clientNode: ActorRef) => this.client = clientNode
    case AddOperator(operator: ActorRef) =>
      this.operators = operators.+(operator)
      log.info(s"added operator ${operator.path.name} on ${operator.path.address.host.getOrElse("self")} to operators (now ${operators.size} total)")
    case RemoveOperator(operator: ActorRef) =>
      this.operators = operators.-(operator)
      log.info(s"removed operator ${operator.path.name}, now ${operators.size} total")
    case AddBackupOperator(operator: ActorRef) => this.backupOperators = backupOperators.+(operator)
    case RemoveBackupOperator(operator: ActorRef) => this.backupOperators = backupOperators.-(operator)
    case GetOperators => sender() ! this.operators.toList
    case GetBackupOperators => sender() ! this.backupOperators.toList
    case GetOperatorCount => sender() ! this.operators.size
    case NotifyOperators(msg: TransitionControlMessage) =>
      log.info(s"broadcasting message $msg to ${operators.size} operators: \n ${operators.mkString("\n")}")
      operators.foreach { op => op ! msg }
    case GetAverageLatency => sender() ! this.latencyMovingAverage
    case UpdateLatency(latency) => latencyMovingAverage = this.calculateMovingAvg(latency)
    case TransitionStatsSingle(operator, timetaken, placementOverheadBytes, transitionOverheadBytes) =>
      SpecialStats.log(this.getClass.toString, s"transitionStats-perOperator-$transitionConfig-${currentPlacementStrategy.name}",
        s"operator;$operator;$timetaken;ms;${placementOverheadBytes / 1000.0};kByte;${transitionOverheadBytes / 1000.0};kByte;${(transitionOverheadBytes + placementOverheadBytes) / 1000.0};kByte")

  }

  /**
    * calculate average latency over the past minute
    */
  def calculateMovingAvg(currentLatency: Long): Double = {
    val lastMinuteValues = previousLatencies.filter(System.currentTimeMillis() - _._1 <= 60 * 1000).updated(System.currentTimeMillis(), currentLatency)
    val avg = lastMinuteValues.values.sum.toDouble / lastMinuteValues.size
    this.previousLatencies = lastMinuteValues
    avg
  }

}

abstract class MAPEK(context: ActorContext) {
  val monitor: ActorRef
  val analyzer: ActorRef
  val planner: ActorRef
  val executor: ActorRef
  val knowledge: ActorRef

  def stop(): Unit = {
    monitor ! PoisonPill
    analyzer ! PoisonPill
    planner ! PoisonPill
    executor ! PoisonPill
    knowledge ! PoisonPill
  }
}

object MAPEK {

  def createMAPEK(mapekType: String, context: ActorContext, query: Query, transitionConfig: TransitionConfig, publishers: Map[String, ActorRef], startingPlacementStrategy: Option[PlacementStrategy], allRecords: Option[AllRecords], consumer: ActorRef, fixedSimulationProperties: Map[Symbol, Int] = Map()): MAPEK = {

    val placementAlgorithm = // get correct PlacementAlgorithm case class for both cases (explicit starting algorithm and implicit via requirements)
      if(startingPlacementStrategy.isEmpty) BenchmarkingNode.selectBestPlacementAlgorithm(List(), Queries.pullRequirements(query, List()).toList) // implicit
      else BenchmarkingNode.algorithms.find(_.placement.name == startingPlacementStrategy.getOrElse(PietzuchAlgorithm).name).getOrElse( // explicit
        throw new IllegalArgumentException(s"missing configuration in application.conf for algorithm $startingPlacementStrategy"))

    assert(mapekType == "lightweight" || mapekType == "requirementBased", s"mapekType must be either lightweight or requirementBased: $mapekType")
    mapekType match {
        // if no algorithm is specified, start with pietzuch, since no context information available yet
      //case "CONTRAST" => new ContrastMAPEK(context, query, mode, startingPlacementStrategy.getOrElse(PietzuchAlgorithm), publishers, fixedSimulationProperties, allRecords.getOrElse(AllRecords()))
      case "requirementBased" => new RequirementBasedMAPEK(context, query, transitionConfig, placementAlgorithm)
      case "lightweight" => new LightweightMAPEK(context, query, transitionConfig, placementAlgorithm.placement, /*, allRecords.getOrElse(AllRecords())*/  consumer)
    }

  }

  // message for inter-MAPEK-component communication
  case class AddRequirement(requirements: Seq[Requirement])
  case class RemoveRequirement(requirements: Seq[Requirement])
  case class ManualTransition(algorithmName: String)
  case class ExecuteTransition(algorithmName: String)
  case object GetRequirements
  case object GetTransitionMode
  case class SetTransitionMode(mode: TransitionConfig)
  case object GetTransitionStatus
  case class SetTransitionStatus(status: Int)
  case object GetLastTransitionStats
  case object GetLastTransitionDuration
  case class SetLastTransitionStats(stats: TransitionStats)
  case object GetLastTransitionEnd
  case object GetClient
  case class SetClient(clientNode: ActorRef)
  case class NotifyOperators(msg: TransitionControlMessage)
  case class AddOperator(ref: ActorRef)
  case class AddBackupOperator(ref: ActorRef)
  case class RemoveOperator(operator: ActorRef)
  case class RemoveBackupOperator(operator: ActorRef)
  case object GetOperators
  case object GetBackupOperators
  case object GetOperatorCount
  case object GetPlacementStrategyName
  case class SetPlacementStrategy(strategy: PlacementStrategy)
  case object IsDeploymentComplete
  case class SetDeploymentStatus(complete: Boolean)
  case object GetAverageLatency
  case class UpdateLatency(latency: Long)
}

case class TransferState(newActor: ActorRef)

case class ChangeInNetwork(networkProperty: NetworkChurnRate)
case class ScheduleTransition(strategy: PlacementStrategy)

case class TransitionRequest(placementStrategy: PlacementStrategy, requester: ActorRef, transitionStats: TransitionStats) extends TransitionControlMessage
case class StopExecution() extends TransitionControlMessage
case class StartExecution(algorithmType: String) extends TransitionControlMessage
case class SaveStateAndStartExecution(state: List[Any]) extends TransitionControlMessage
case class StartExecutionWithData(downTime:Long, startTime: Long, subscribers: Set[ActorRef], data: Set[(ActorRef, Any)], algorithmType: String) extends TransitionControlMessage
case class StartExecutionAtTime(subscribers: List[ActorRef], startTime: Instant, algorithmType: String) extends TransitionControlMessage
case class TransferredState(placementAlgo: PlacementStrategy, newParent: ActorRef, oldParent: ActorRef, transitionStats: TransitionStats) extends TransitionControlMessage
// only used inside TransitionRequest and TransferredState; transitionOverheadBytes must be updated on receive of TransitionRequest and TransferredState, placementOverheadBytes on operator placement completion
case class TransitionStats(
                            placementOverheadBytes: Long = 0, transitionOverheadBytes: Long = 0, transitionStartAtKnowledge: Long = System.currentTimeMillis(),
                            transitionTimesPerOperator: Map[ActorRef, Long] = Map(), // record transition duration per operator
                            transitionEndParent: Long = System.currentTimeMillis()) // for SMS mode delay estimation
case class TransitionStatsSingle(operator: ActorRef, timetaken: Long, placementOverheadBytes: Long, transitionOverheadBytes: Long)
case class TransitionDuration(operator: ActorRef, timetaken: Long)