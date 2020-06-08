package tcep.graph.nodes.traits

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, PoisonPill}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
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
import scala.concurrent.ExecutionContext

/**
  * Handling of [[tcep.data.Queries.BinaryQuery]] is done by BinaryNode
  **/
trait BinaryNode extends Node {
  override val query: BinaryQuery
  var parentNode1: ActorRef
  var parentNode2: ActorRef
  val p1List: ListBuffer[ActorRef] = ListBuffer()
  val p2List: ListBuffer[ActorRef] = ListBuffer()
  var childNode1Created: Boolean = false
  var childNode2Created: Boolean = false

  private var parent1TransitInProgress = false
  private var parent2TransitInProgress = false
  private var selfTransitionStarted = false
  private var transitionRequestor: ActorRef = _
  private var transitionStartTime: Long = _
  private var transitionStatsAcc: TransitionStats = TransitionStats()

  override def preStart(): Unit = {
    super.preStart()
    p1List += parentNode1
    p2List += parentNode2
  }

  override def subscribeToParents(): Unit = {
    implicit val resolveTimeout = Timeout(15, TimeUnit.SECONDS)
    for {
      sub1 <- TCEPUtils.guaranteedDelivery(context, parentNode1, Subscribe(self))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
      sub2 <- TCEPUtils.guaranteedDelivery(context, parentNode2, Subscribe(self))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
    } yield {
      log.info(s"$self \n subscribed for events from \n${parentNode1.toString()} and \n${parentNode2.toString()} \n parent list: \n ${p1List} \n ${p2List}")
      transitionLog(s"successfully subscribed to ${p1List.size + p2List.size} parents ")
    }
  }

  // child receives control back from parent that has completed their transition
  def handleTransferredState(algorithm: PlacementStrategy, newParent: ActorRef, oldParent: ActorRef, stats: TransitionStats): Unit = {
    implicit val ec: ExecutionContext = blockingIoDispatcher
    if(!selfTransitionStarted) {
      transitionConfig.transitionExecutionMode match {
        case TransitionExecutionModes.CONCURRENT_MODE => {

          if (p1List.contains(oldParent)) {
            p1List += newParent
            p1List -= oldParent
            parentNode1 = newParent
            parent1TransitInProgress = false
          } else if (p2List.contains(oldParent)) {
            p2List += newParent
            p2List -= oldParent
            parentNode2 = newParent
            parent2TransitInProgress = false
          } else {
            transitionLog(s"received TransferState msg from non-parent: ${oldParent}; parents: ${p1List.map(_.path.name)} ${p2List.map(_.path.name)}")
            log.error(new IllegalStateException(s"received TransferState msg from non-parent: ${oldParent}; \n parents: \n $p1List \n $p2List"), "TRANSITION ERROR")
          }
        }

        case TransitionExecutionModes.SEQUENTIAL_MODE => {
          if (p1List.contains(oldParent)) {
            p1List += newParent
            p1List -= oldParent
            parent1TransitInProgress = false
            parentNode1 = newParent
            TCEPUtils.guaranteedDelivery(context, p2List.last, TransitionRequest(algorithm, self, stats), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
            parent2TransitInProgress = true

          } else if (p2List.contains(oldParent)) {
            p2List += newParent
            p2List -= oldParent
            parent2TransitInProgress = false
            parentNode2 = newParent
          } else {
            transitionLog(s"received TransferState msg from non-parent: ${oldParent}; parents: ${p1List.map(_.path.name)} ${p2List.map(_.path.name)}")
            log.error(new IllegalStateException(s"received TransferState msg from non-parent: ${oldParent}; \n parents: \n $p1List \n $p2List"), "TRANSITION ERROR")
          }
        }
      }

      accumulateTransitionStats(stats)

      if (!parent1TransitInProgress && !parent2TransitInProgress) {
        selfTransitionStarted = true
        log.info(s"parents transition complete, executing own transition -\n new parents: ${parentNode1.toString()} ${parentNode2.toString()}")
        transitionLog(s"old parents transition to new parents with ${algorithm.name} complete, executing own transition")
        executeTransition(transitionRequestor, algorithm, transitionStatsAcc)
      }
    }
  }

  override def childNodeReceive: Receive = {
    case DependenciesRequest => sender ! DependenciesResponse(Seq(p1List.last, p2List.last))
    case Created if p1List.contains(sender()) => {
      childNode1Created = true
      log.info("received Created from parent1")
      if (childNode2Created) emitCreated()
    }
    case Created if p2List.contains(sender()) => {
      childNode2Created = true
      log.info("received Created from parent2")
      if (childNode1Created) emitCreated()
    }

    //parent has transited to the new node
    case TransferredState(algorithm, newParent, oldParent, stats) => {
      sender() ! ACK()
      handleTransferredState(algorithm, newParent, oldParent, stats)
    }

    case OperatorMigrationNotice(oldOperator, newOperator) => { // received from migrating parent (oldParent)
      if(p1List.contains(oldOperator)) {
        p1List -= oldOperator
        p1List += newOperator
        parentNode1 = newOperator
      }
      if(p2List.contains(oldOperator)) {
        p2List -= oldOperator
        p2List += newOperator
        parentNode2 = newOperator
      }
      TCEPUtils.guaranteedDelivery(context, newOperator, Subscribe(self))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
      log.info(s"received operator migration notice from ${oldOperator}, \n new operator is $newOperator \n updated parents $p1List $p2List")
    }

    case ShutDown() => {
      p1List.last ! ShutDown()
      p2List.last ! ShutDown()
      self ! PoisonPill
    }
  }

  override def handleTransitionRequest(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): Unit = {
    implicit val ec: ExecutionContext = blockingIoDispatcher
    transitionLog(s"asking old parents ${p1List.last} and ${p2List.last} to transit to new parents with ${algorithm.name}")
    transitionStartTime = System.currentTimeMillis()
    transitionRequestor = requester
    transitionStatsAcc = TransitionStats()
    transitionConfig.transitionExecutionMode match {
      case TransitionExecutionModes.CONCURRENT_MODE => {
        TCEPUtils.guaranteedDelivery(context, p1List.last, TransitionRequest(algorithm, self, stats), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
        TCEPUtils.guaranteedDelivery(context, p2List.last, TransitionRequest(algorithm, self, stats), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
        parent1TransitInProgress = true
        parent2TransitInProgress = true
      }
      case TransitionExecutionModes.SEQUENTIAL_MODE => {
        TCEPUtils.guaranteedDelivery(context, p1List.last, TransitionRequest(algorithm, self, stats), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
        parent1TransitInProgress = true
        parent2TransitInProgress = false
      }

      case default => throw new Error("Invalid Configuration. Stopping execution of program")
    }
  }

  // merge transition stats from parents for a binary operator
  def accumulateTransitionStats(stats: TransitionStats): TransitionStats = {
    transitionStatsAcc = TransitionStats(
      transitionStatsAcc.placementOverheadBytes + stats.placementOverheadBytes,
      transitionStatsAcc.transitionOverheadBytes + stats.transitionOverheadBytes,
      stats.transitionStartAtKnowledge,
      transitionStatsAcc.transitionTimesPerOperator ++ stats.transitionTimesPerOperator,
      math.max(transitionStatsAcc.transitionEndParent, stats.transitionEndParent)
    )
    transitionStatsAcc
  }

  def getParentOperators(): Seq[ActorRef] = Seq(p1List.last, p2List.last)

}
