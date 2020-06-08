package tcep.graph.nodes.traits

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, PoisonPill}
import akka.util.Timeout
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.nodes.ShutDown
import tcep.graph.nodes.traits.Node.{OperatorMigrationNotice, Subscribe}
import tcep.graph.transition.{TransferredState, TransitionRequest, TransitionStats}
import tcep.machinenodes.helper.actors.ACK
import tcep.placement.PlacementStrategy
import tcep.publishers.Publisher.AcknowledgeSubscription
import tcep.utils.TCEPUtils

import scala.collection.mutable.ListBuffer

/**
  * Handling of [[tcep.data.Queries.UnaryQuery]] is done by UnaryNode
  **/
trait UnaryNode extends Node {

  override val query: UnaryQuery
  private var transitionRequestor: ActorRef = _
  private var selfTransitionStarted = false
  private var transitionStartTime: Long = _
  var parentNode: ActorRef
  val parentsList: ListBuffer[ActorRef] = ListBuffer() //can have multiple active unary parents due to SMS mode (we could receive messages from older parents)
  override def preStart(): Unit = {
    super.preStart()
    parentsList += parentNode
  }

  override def subscribeToParents(): Unit = {
    implicit val resolveTimeout = Timeout(10, TimeUnit.SECONDS)
    for {
      ack <- TCEPUtils.guaranteedDelivery(context, parentNode, Subscribe(self))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
    } yield {
      log.info(s"subscribing for events from ${parentNode.path.name}, \n parentList: $parentsList")
    }
  }

  override def childNodeReceive: Receive = {

    case DependenciesRequest => sender() ! DependenciesResponse(Seq(parentsList.last))
    case Created =>
      log.info(s"received CREATED from ${sender()}, parentsList: $parentsList")
      if(parentsList.contains(sender())) emitCreated()

    case TransferredState(placementAlgo, successor, oldParent, stats) => {
      val s = sender()
      s ! ACK()
      if(!selfTransitionStarted) {
        selfTransitionStarted = true
        if (parentsList.contains(oldParent)) {
          parentsList += successor
          parentsList -= oldParent
          parentNode = successor
        } else log.error(new IllegalStateException(s"received TransferState msg from non-parent: ${oldParent}; \n parents: \n $parentsList"), "TRANSITION ERROR")

        transitionLog(s"old parent transition to new parent with ${placementAlgo.name} complete, executing own transition")
        executeTransition(transitionRequestor, placementAlgo, stats) // stats are updated by MFGS/SMS receive of TransferredState
      }
    }

    case OperatorMigrationNotice(oldOperator, newOperator) => { // received from migrating parent

      if(parentsList.contains(oldOperator)) {
        parentsList -= oldOperator
        parentsList += newOperator
        parentNode = newOperator
      }
      TCEPUtils.guaranteedDelivery(context, newOperator, Subscribe(self))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
      log.info(s"received operator migration notice from ${oldOperator}, \n new operator is $newOperator \n updated parent $parentsList")
    }

    case ShutDown() => {
      parentsList.last ! ShutDown()
      self ! PoisonPill
    }

  }

  override def handleTransitionRequest(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): X = {
    log.info(s"Asking ${parentsList.last.path.name} to transit to algorithm ${algorithm.name}")
    transitionLog(s"asking old parent ${parentsList.last.path} to transit to new parent with ${algorithm.name}")
    transitionRequestor = requester
    transitionStartTime = System.currentTimeMillis()
    TCEPUtils.guaranteedDelivery(context, parentsList.last, TransitionRequest(algorithm, self, stats), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))(blockingIoDispatcher)
  }

  override def getParentOperators(): Seq[ActorRef] = Seq(parentsList.last)
}