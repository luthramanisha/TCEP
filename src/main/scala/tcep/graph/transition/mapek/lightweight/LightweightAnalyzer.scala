package tcep.graph.transition.mapek.lightweight

import akka.actor.{ActorRef, Cancellable}
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import tcep.graph.transition.AnalyzerComponent
import tcep.graph.transition.MAPEK.{GetLastTransitionEnd, GetTransitionStatus, IsDeploymentComplete}
import tcep.graph.transition.mapek.lightweight.LightweightAnalyzer.{InitTransition, NodeUnreachable}
import tcep.graph.transition.mapek.lightweight.LightweightMAPEK.GetConsumer
import tcep.graph.transition.mapek.lightweight.LightweightPlanner.Plan
import tcep.machinenodes.consumers.Consumer.GetAllRecords
import tcep.simulation.tcep.AllRecords

import scala.concurrent.Future
import scala.concurrent.duration._

class LightweightAnalyzer(mapek: LightweightMAPEK) extends AnalyzerComponent{

  var planningScheduler: Cancellable = _
  val transitionCooldown = ConfigFactory.load().getInt("constants.mapek.transition-cooldown") * 1000
  var denied = 0

  override def preStart(): Unit = {
    super.preStart()
    log.info("Staring Lightweight Analyzer")
    planningScheduler = this.context.system.scheduler.schedule(1 minute, 1 minute, this.self, InitTransition)
  }

  override def receive: Receive = {
    case InitTransition =>
      log.info("INIT Transition Message received")
      for {
        consumer <- (mapek.knowledge ? GetConsumer).mapTo[ActorRef]
        allRecords <- { log.info(s"consumer $consumer found"); (consumer ? GetAllRecords).mapTo[AllRecords]}
        lastTransitionEnd <- { log.info(s"Records defined? ${allRecords.allDefined}"); (mapek.knowledge ? GetLastTransitionEnd).mapTo[Long] }
        deploymentComplete <- { log.info(s"Cooldown exceeded? ${System.currentTimeMillis() - lastTransitionEnd >= transitionCooldown}"); (mapek.knowledge ? IsDeploymentComplete).mapTo[Boolean] }
        transitionStatus  <- { log.info(s"deployment is complete: $deploymentComplete"); (mapek.knowledge ? GetTransitionStatus).mapTo[Int]}
        logg: Boolean <- {
          log.info(s"LastTransitionEnd is $lastTransitionEnd")
          log.info(s"deployment is complete: $deploymentComplete")
          log.info(s"Transition status is $transitionStatus")
          log.info(s"Cooldown exceeded? ${System.currentTimeMillis() - lastTransitionEnd >= transitionCooldown}")
          log.info(s"Records defined? ${allRecords.allDefined}")
          Future { true }
        }
        //if deploymentComplete && transitionStatus == 0 && System.currentTimeMillis() - lastTransitionEnd >= transitionCooldown && allRecords.allDefined
      } yield {
        if (deploymentComplete && transitionStatus == 0 && System.currentTimeMillis() - lastTransitionEnd >= transitionCooldown && allRecords.allDefined) {
          mapek.planner ! Plan
          log.info("notifying Planner")
          denied = 0
        }/* else {
          denied += 1
          if (denied >= 1) {
            log.info("RESETTING TransitionStatus")
            mapek.knowledge ! SetTransitionStatus(0)
            mapek.planner ! ResetPlacement
          }
        }*/
      }
      /*if (denied > 1) {
        denied = 0
        mapek.knowledge ! SetTransitionStatus(0)
        log.info("RESETTING TransitionStatus")
      }*/

    case NodeUnreachable =>
      log.info("Operator Host down. Transitioning...")
      mapek.planner ! Plan
  }
}
object LightweightAnalyzer{

  case class InitTransition()
  case class NodeUnreachable()
}