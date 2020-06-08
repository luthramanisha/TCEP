package tcep.graph.nodes.traits

import akka.actor.{ActorRef, PoisonPill}
import tcep.data.Events.{Created, DependenciesRequest, DependenciesResponse}
import tcep.data.Queries.LeafQuery
import tcep.graph.nodes.ShutDown
import tcep.graph.transition.{StartExecution, TransitionRequest, TransitionStats}
import tcep.graph.{CreatedCallback, EventCallback}
import tcep.machinenodes.helper.actors.ACK
import tcep.placement.PlacementStrategy

/**
  * Handling of [[tcep.data.Queries.LeafQuery]] is done by LeafNode
  **/
trait LeafNode extends Node {

  val query: LeafQuery

  override def childNodeReceive: Receive = {
    case Created => emitCreated()
    case DependenciesRequest => sender ! DependenciesResponse(Seq.empty)
    case ShutDown() => {
      self ! PoisonPill
    }

  }

  override def handleTransitionRequest(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): Unit =
    executeTransition(requester, algorithm, stats)
}
