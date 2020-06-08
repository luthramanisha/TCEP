package tcep.utils

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.actor.Actor.Receive
import akka.cluster.Cluster
import scala.concurrent.duration._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import scala.concurrent.ExecutionContext.Implicits.global
import tcep.utils.TransitionLogActor.{OperatorTransitionBegin, OperatorTransitionEnd, TransitionMessage}
object TransitionLogActor {
  val topicName: String = "TransitionDebug"
  case class TransitionMessage(timestamp: String, msg: String)
  case class OperatorTransitionBegin(actor: ActorRef)
  case class OperatorTransitionEnd(actor: ActorRef)
}

class TransitionLogSubscriber extends Actor with ActorLogging {

  var ongoingTransitions: Set[ActorRef] = Set()
  val topic = TransitionLogActor.topicName
  import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
  val mediator = DistributedPubSub(context.system).mediator
  // subscribe to the topic named "content"
  mediator ! Subscribe(topic, self)
  context.system.scheduler.schedule(10 seconds, 10 seconds, () => SpecialStats.log(this.getClass.toString, "ongoingTransitions", s"${ongoingTransitions.size} operator transitions not yet complete: $ongoingTransitions"))

  override def receive: Receive = {
    case tMsg: TransitionMessage => SpecialStats.log(s"${sender()}", "transitionLogger", msg=tMsg.msg, timestamp=tMsg.timestamp)
    case OperatorTransitionBegin(actor) => ongoingTransitions = ongoingTransitions.+(actor)
    case OperatorTransitionEnd(actor) => ongoingTransitions = ongoingTransitions.-(actor)
    case SubscribeAck(Subscribe(`topic`, None, `self`)) =>
      log.info(s"TransitionLogActor subscribing to topic $topic")
    case msg => log.info(s"received unknown msg $msg from ${sender()}")
  }

}

class TransitionLogPublisher extends Actor with ActorLogging {
  val topic = TransitionLogActor.topicName
  import akka.cluster.pubsub.DistributedPubSubMediator.Publish
  // activate the extension
  val mediator = DistributedPubSub(context.system).mediator

  def receive = {
    case in: String =>
      val tMsg = TransitionMessage(SpecialStats.getTimestamp, in)
      mediator ! Publish(topic, tMsg)
    case begin: OperatorTransitionBegin => mediator ! Publish(topic, begin)
    case end: OperatorTransitionEnd => mediator ! Publish(topic, end)
  }
}