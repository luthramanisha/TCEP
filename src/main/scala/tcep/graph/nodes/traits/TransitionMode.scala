package tcep.graph.nodes.traits

import java.time.Duration

import akka.actor.{ActorLogging, ActorRef, ActorSelection, Terminated}
import akka.cluster.Cluster
import tcep.ClusterActor
import tcep.graph.nodes.traits.Node.{Subscribe, UnSubscribe}
import tcep.graph.transition.MAPEK.{AddOperator, RemoveOperator}
import tcep.graph.transition._
import tcep.machinenodes.helper.actors.{ACK, CreateRemoteOperator, RemoteOperatorCreated}
import tcep.placement.{HostInfo, PlacementStrategy}
import tcep.publishers.Publisher.AcknowledgeSubscription
import tcep.utils.{SizeEstimator, SpecialStats, TCEPUtils}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
/**
  * Common methods for different transition modes
  */
trait TransitionMode extends ClusterActor with ActorLogging {

  val modeName: String = "transition-mode name not specified"
  val subscribers: mutable.Set[ActorRef]
  val isRootOperator: Boolean

  @volatile var started: Boolean
  val backupMode: Boolean
  val mainNode: Option[ActorRef]
  val hostInfo: HostInfo
  var transitionInitiated = false


  def createDuplicateNode(hostInfo: HostInfo): Future[ActorRef]
  def subscribeToParents()
  def getParentOperators(): Seq[ActorRef]
  def maxWindowTime(): Duration = Duration.ofSeconds(0)

  // Notice I'm using `PartialFunction[Any,Any]` which is different from akka's default
  // receive partial function which is `PartialFunction[Any,Unit]`
  // because I needed to `cache` some messages before forwarding them to the child nodes
  // @see the receive method in Node trait for more details.
  def transitionReceive: PartialFunction[Any, Any] = {
    case Subscribe(sub) => {
      subscribers += sub
      transitionLog(s"new subscription by ${sub.path.name}, now ${subscribers.size} subscribers")
      log.info(s"self ${this.self.path.name} \n was subscribed by ${sub.path.name}, \n now has ${subscribers.size} subscribers")
      sender() ! AcknowledgeSubscription()
    }

    case UnSubscribe() => {
      val s = sender()
      subscribers -= s
      transitionLog(s"unsubscribed by ${s.path.name}, now ${subscribers.size} subscribers")
      log.info(s"self ${this.self.path.name} was unsubscribed by ${s.path.name}, \n now has ${subscribers.size} subscribers")
    }

   case TransitionRequest(algorithm, requester, stats) => {
      val s = sender()
      s ! ACK()
      if(!transitionInitiated){
        transitionInitiated = true
        handleTransitionRequest(requester, algorithm, updateTransitionStats(stats, s, transitionRequestSize(s)))
      }
    }

    case TransferredState(algorithm, successor, oldParent, stats) => {
      val s = sender() // this is a temporary deliverer actor created by oldParent
      s ! ACK()
      val transitionStart: Long = stats.transitionTimesPerOperator.getOrElse(oldParent, 0)
      val transitionDuration = System.currentTimeMillis() - transitionStart
      val opmap = stats.transitionTimesPerOperator.updated(oldParent, transitionDuration)
      log.info(s"old parent $oldParent  \n transition start: $transitionStart, transitionDuration: ${transitionDuration} , operator stats: \n ${opmap.mkString("\n")}")
      TransferredState(algorithm, successor, oldParent, updateTransitionStats(stats, oldParent, transferredStateSize(oldParent), updatedOpMap = Some(opmap) )) //childReceive will handle this message
    }

    case Terminated(killed) => {
      if (killed.equals(mainNode.get)) {
        started = true
      }
    }

    case unhandled => {
      unhandled //forwarding to childreceive
    }
  }

  def handleTransitionRequest(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): Unit

  def executeTransition(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): Unit

  def sendTransitionStats(operator: ActorRef, transitionTime: Long, placementBytes: Long, transitionBytes: Long) = {
    implicit val timeout = TCEPUtils.timeout
    for {
      simulator <- TCEPUtils.selectSimulator(cluster).resolveOne().mapTo[ActorRef]
      knowledgeActor <- ActorSelection(simulator, "knowledge*").resolveOne().mapTo[ActorRef]
    } yield {
      knowledgeActor ! TransitionStatsSingle(self, transitionTime, placementBytes, transitionBytes)
    }
  }

  def notifyMAPEK(cluster: Cluster, successor: ActorRef) = {
    implicit val timeout = TCEPUtils.timeout
    for {
      simulator <- TCEPUtils.selectSimulator(cluster).resolveOne().mapTo[ActorRef]
      knowledgeActor <- ActorSelection(simulator, "knowledge*").resolveOne().mapTo[ActorRef]
    } yield {
      log.info(s"notifying MAPEK knowledge component ${knowledgeActor} about changed operator ${successor}")
      knowledgeActor ! AddOperator(successor)
      knowledgeActor ! RemoveOperator(self)
    }
  }
}