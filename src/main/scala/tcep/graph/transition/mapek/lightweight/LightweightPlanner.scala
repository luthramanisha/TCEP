package tcep.graph.transition.mapek.lightweight

import akka.pattern.ask
import tcep.graph.transition.MAPEK.{ExecuteTransition, GetPlacementStrategyName}
import tcep.graph.transition.PlannerComponent
import tcep.graph.transition.mapek.lightweight.LightweightKnowledge.GetTransitionData
import tcep.graph.transition.mapek.lightweight.LightweightPlanner.{Plan, ResetPlacement}



class LightweightPlanner(mapek: LightweightMAPEK) extends PlannerComponent(mapek){


  override def preStart(): Unit = {
    super.preStart()
    log.info("Staring Lightweight Planner")
  }

  override def receive: Receive = {
    case Plan =>
      log.info("Plan message received")
      for {
        tmp <- (mapek.knowledge ? GetTransitionData).mapTo[String]
        current <- (mapek.knowledge ? GetPlacementStrategyName).mapTo[String]
        if !tmp.equals(current)
      } yield {

        log.info(s"Received $tmp")
        mapek.executor ! ExecuteTransition(tmp)
      }

    case ResetPlacement =>
      log.info("Resetting Placement!")
      for {
        current <- (mapek.knowledge ? GetPlacementStrategyName).mapTo[String]
      } yield {
        log.info(s"Received $current")
        mapek.executor ! ExecuteTransition(current)
      }
  }
}
object LightweightPlanner{
  case class Plan()
  case object ResetPlacement
}
