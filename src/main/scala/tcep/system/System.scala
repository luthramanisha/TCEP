package tcep.system

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorContext, ActorRef}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.util.Timeout
import tcep.data.Events.{DependenciesRequest, DependenciesResponse}
import tcep.data.Queries._
import tcep.graph.nodes.traits.{TransitionConfig, TransitionModeNames}
import tcep.graph.qos._
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object System {
  val index = new AtomicInteger(0)
}

class System(context: ActorContext) {
  private val roots = ListBuffer.empty[ActorRef]
  private val placements = mutable.Map.empty[ActorRef, Host] withDefaultValue NoHost

  def runQuery(query: Query, publishers: Map[String, ActorRef], createdCallback: Option[CreatedCallback], eventCallback: Option[EventCallback]) = {
    val monitorFactories: Array[MonitorFactory] = Array(AverageFrequencyMonitorFactory(query, Option.empty),
      DummyMonitorFactory(query))
    val graphFactory = new QueryGraph(this.context, Cluster(context.system), query, TransitionConfig(), publishers, None, createdCallback, monitorFactories)

    roots += graphFactory.createAndStart(null)(eventCallback)
  }

  def consumers: Seq[Operator] = {
    import context.dispatcher
    implicit val timeout = Timeout(20.seconds)

    def operator(actorRef: ActorRef): Future[Operator] =
      actorRef ? DependenciesRequest flatMap {
        case DependenciesResponse(dependencies) =>
          Future sequence (dependencies map operator) map {
            ActorOperator(placements(actorRef), _, actorRef)
          }
      }

    Await result(Future sequence (roots map operator), timeout.duration)
  }

  def place(operator: Operator, host: Host) = {
    val ActorOperator(_, _, actorRef) = operator
    placements += actorRef -> host
  }

  private case class ActorOperator(host: Host, dependencies: Seq[Operator], actorRef: ActorRef) extends Operator

}
