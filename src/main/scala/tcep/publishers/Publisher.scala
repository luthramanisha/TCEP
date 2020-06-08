package tcep.publishers

import akka.actor.{ActorLogging, ActorRef}
import akka.cluster.ClusterEvent._
import tcep.graph.nodes.traits.Node._
import tcep.graph.transition.{StartExecution, TransitionRequest}
import tcep.machinenodes.helper.actors.PlacementMessage
import tcep.placement.vivaldi.VivaldiCoordinates
import tcep.publishers.Publisher._

import scala.collection.mutable

/**
  * Publisher Actor
  **/
trait Publisher extends VivaldiCoordinates with ActorLogging {

  var subscribers: mutable.Set[ActorRef] = mutable.Set.empty[ActorRef]

  override def receive: Receive = super.receive orElse {
    case Subscribe(sub) =>
      log.info(s"${sub} subscribed to publisher $self, current subscribers: \n ${subscribers.mkString("\n")}")
      subscribers += sub
      sender() ! AcknowledgeSubscription()

    case UnSubscribe() =>
      log.info(s"${sender().path.name} unSubscribed")
      subscribers -= sender()

    case _: MemberEvent => // ignore
    case StartExecution(algorithm) => // meant for operators, but is forwarded anyway
    case _: TransitionRequest => log.error(s"FAULTY SUBSCRIPTION: \n Publisher ${self} received TransitionRequest from $sender()")
  }

}

/**
  * List of Akka Messages which is being used by Publisher actor.
  **/
object Publisher {
  case class AcknowledgeSubscription() extends PlacementMessage
  case class StartStreams() extends PlacementMessage
}
