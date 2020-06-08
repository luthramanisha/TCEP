package tcep.graph.transition.mapek.lightweight

import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, ActorRef, Props}
import com.typesafe.config.ConfigFactory
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.transition.MAPEK
import tcep.placement.PlacementStrategy

import scala.concurrent.duration.FiniteDuration

class LightweightMAPEK(context: ActorContext, query: Query,transitionConfig: TransitionConfig, startingPlacementStrategy: PlacementStrategy/*, allRecords: AllRecords*/, consumer: ActorRef) extends MAPEK(context) {

  val samplingInterval = new FiniteDuration(ConfigFactory.load().getInt("constants.mapek.sampling-interval"), TimeUnit.SECONDS)
  val monitor: ActorRef = context.actorOf(Props(new LightweightMonitor(this)))
  val analyzer: ActorRef = context.actorOf(Props(new LightweightAnalyzer(this)))
  val planner: ActorRef = context.actorOf(Props(new LightweightPlanner(this)))
  val executor: ActorRef = context.actorOf(Props(new LightweightExecutor(this)))
  val knowledge: ActorRef = context.actorOf(Props(new LightweightKnowledge(this, query, transitionConfig, startingPlacementStrategy, consumer)))
}

object LightweightMAPEK {
  case class SetLastTransitionEnd(time: Long)
  case object GetConsumer
}