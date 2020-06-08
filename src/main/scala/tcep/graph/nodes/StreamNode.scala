package tcep.graph.nodes

import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import tcep.data.Events._
import tcep.data.Queries._
import tcep.factories.NodeFactory
import tcep.graph.nodes.traits.Node.{Subscribe, UnSubscribe}
import tcep.graph.nodes.traits.TransitionModeNames._
import tcep.graph.nodes.traits._
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}
import tcep.machinenodes.helper.actors.{CreateRemoteOperator, RemoteOperatorCreated}
import tcep.placement.HostInfo
import tcep.publishers.Publisher._
import tcep.utils.TCEPUtils

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

/**
  * Handling of [[tcep.data.Queries.StreamQuery]] is done by StreamNode.
  *
  * @see [[QueryGraph]]
  **/

case class StreamNode(transitionConfig: TransitionConfig,
                      hostInfo: HostInfo,
                      backupMode: Boolean,
                      mainNode: Option[ActorRef],
                      query: StreamQuery,
                      createdCallback: Option[CreatedCallback],
                      eventCallback: Option[EventCallback],
                      isRootOperator: Boolean,
                      publisher: ActorRef*
                     )
  extends LeafNode with ActorLogging {

  override def subscribeToParents(): Unit = {
    implicit val resolveTimeout = Timeout(10, TimeUnit.SECONDS)
    for {
      ack <- TCEPUtils.guaranteedDelivery(context, publisher.head, Subscribe(self))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
    } yield {
      log.info(s"subscribed for events from ${publisher.head.path.name}")
      context.system.scheduler.scheduleOnce(FiniteDuration(100, TimeUnit.MILLISECONDS), self, Created) // delay sending created so all children can subscribe to avoid failing tests
    }
  }

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event if sender().equals(publisher.head) => emitEvent(event)
    case unhandledMessage => log.info(s"${self.path.name} unhandled message ${unhandledMessage.getClass} by ${sender()}, publisher is ${publisher.head}")
  }

  def getParentOperators(): Seq[ActorRef] = publisher
}

