package tcep.graph.transition.mapek.requirementBased

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorContext, ActorRef, Cancellable, Props}
import akka.cluster.ClusterEvent.{MemberJoined, MemberLeft}
import akka.pattern.ask
import akka.util.Timeout
import tcep.data.Queries._
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.transition.MAPEK._
import tcep.graph.transition._
import tcep.graph.transition.mapek.requirementBased.RequirementBasedMAPEK.{IsRequirementSupportedByStrategy, SetPlacementAlgorithm}
import tcep.placement.benchmarking._

import scala.concurrent.duration._

/**
  * Created by raheel
  * on 02/10/2017.
  */

class RequirementBasedMAPEK(context: ActorContext, val query: Query, mode: TransitionConfig, startingPlacementStrategy: PlacementAlgorithm) extends MAPEK(context) {
  val monitor: ActorRef = context.actorOf(Props(new Monitor()))
  val analyzer: ActorRef = context.actorOf(Props(new Analyzer()))
  val planner: ActorRef = context.actorOf(Props(new Planner(this)))
  val executor: ActorRef = context.actorOf(Props(new Executor(this)))
  val knowledge: ActorRef = context.actorOf(Props(new Knowledge(query, mode, startingPlacementStrategy)), s"knowledge-${randomAlphaNumericString(6)}")

  // 6 - random alphanumeric
  def randomAlphaNumericString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }


  implicit val timeout = Timeout(5 seconds)

  class Monitor extends MonitorComponent {

    val churnRateThreshold = 3
    var changeRate: AtomicInteger = new AtomicInteger(0)
    var updateStateScheduler: Cancellable = _
    var networkChurnRate: NetworkChurnRate = LowChurnRate //default value

    override def preStart(): Unit = {
      super.preStart()
      log.info("starting MAPEK Monitor")
      updateStateScheduler = this.context.system.scheduler.schedule(5 minutes, 5 minutes, this.self, UpdateState)
    }

    override def receive: Receive = {

      case MemberJoined(member) =>
        log.info(s"new member joined $member")
        changeRate.incrementAndGet()

      case MemberLeft(member) =>
        changeRate.incrementAndGet()

      case r: AddRequirement =>
        log.info(s"received addRequirement $r")
        analyzer ! r

      case r: RemoveRequirement =>
        log.info(s"received removeRequirement $r")
        knowledge ! r

      case UpdateState => updateState()

      case m => log.info(s"ignoring unknown messages $m")
    }

    def updateState(): Unit = {
      // TODO obsolete?
      if (changeRate.intValue() >= churnRateThreshold && networkChurnRate.equals(LowChurnRate)) {
        analyzer ! ChangeInNetwork(HighChurnRate)
        log.info("high churn rate of the system, notifying analyzer")
        networkChurnRate = HighChurnRate
      } else if (changeRate.intValue() < churnRateThreshold && networkChurnRate.equals(HighChurnRate)) {
        log.info(s"low churn rate of the system, notifying analyzer")
        analyzer ! ChangeInNetwork(LowChurnRate)
        networkChurnRate = LowChurnRate
      }

      log.info(s"resetting churnrate ${changeRate.get()}")
      changeRate.set(0)
    }
  }

  class Analyzer extends AnalyzerComponent {

    override def preStart() = {
      super.preStart()
      log.info("starting MAPEK Analyzer")
    }

    override def receive: Receive = {

      case AddRequirement(newRequirements) =>
        log.info(s"received requirement(s) $newRequirements, checking if it is supported by current placement algorithm...")
        for {
          supported <- (knowledge ? IsRequirementSupportedByStrategy(newRequirements)).mapTo[Boolean]
          algorithm <- (knowledge ? GetPlacementStrategyName).mapTo[String]
        } yield {
          if(!supported) {
            log.info("requirement is not supported by current placement algorithm, forwarding to planner")
            planner ! AddRequirement(newRequirements)
          } else log.info(s"requirement \n $newRequirements is supported by current placement algorithm $algorithm, no change necessary")
        knowledge ! AddRequirement(newRequirements)
        }

      case ChangeInNetwork(constraint) =>
        log.info(s"received ChangeInNetwork($constraint), sending to planner")
        planner ! ChangeInNetwork(constraint)
    }
  }

  class Planner(mapek: RequirementBasedMAPEK) extends PlannerComponent(mapek) {

    override def preStart() = {
      super.preStart()
      log.info("starting MAPEK Planner")
    }

    override def receive: Receive = super.receive orElse {
      case AddRequirement(newRequirements) =>
        log.info(s"selecting placement algorithm for new requirements $newRequirements")
        selectAndExecuteAlgorithm(List(), newRequirements.toSet)

      case ChangeInNetwork(constraint) =>
        log.info("applying transition due to change in environmental state")
        selectAndExecuteAlgorithm(List(constraint), Set())
    }

    def selectAndExecuteAlgorithm(constraint: List[NetworkChurnRate], newRequirements: Set[Requirement]) = {
      for { existingRequirements <- (knowledge ? GetRequirements).mapTo[List[Requirement]] } yield {
        val reqList = (existingRequirements.toSet ++ newRequirements).toList
        val algorithm = BenchmarkingNode.selectBestPlacementAlgorithm(constraint, reqList)
        log.info(s"Got new Strategy from Benchmarking Node ${algorithm.placement.getClass}")
        executor ! ExecuteTransition(algorithm.placement.name)
      }
    }
  }

  class Executor(mapek: RequirementBasedMAPEK) extends ExecutorComponent(mapek)

  class Knowledge(query: Query, mode: TransitionConfig, var currentPlacementAlgorithm: PlacementAlgorithm)
    extends KnowledgeComponent(query, mode, currentPlacementAlgorithm.placement) {

    override def preStart() = {
      super.preStart()
      log.info(s"starting MAPEK Knowledge $self")
    }

    override def receive: Receive = super.receive orElse {

      case SetPlacementAlgorithm(placementAlgorithm: PlacementAlgorithm) =>
        this.currentPlacementAlgorithm = placementAlgorithm
        this.currentPlacementStrategy = placementAlgorithm.placement

      case r: IsRequirementSupportedByStrategy => sender() ! r.requirements.map(currentPlacementAlgorithm.containsDemand(_)).fold(true)(_ && _)
    }
  }

}

object RequirementBasedMAPEK {
  case class SetPlacementAlgorithm(placementAlgorithm: PlacementAlgorithm)
  case class IsRequirementSupportedByStrategy(requirements: Seq[Requirement])
  case object UpdateState
}