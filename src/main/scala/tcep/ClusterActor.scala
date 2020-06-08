package tcep

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberEvent
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import tcep.graph.nodes.traits.Node.{Subscribe, UnSubscribe}
import tcep.graph.transition.{TransferredState, TransitionRequest, TransitionStats}
import tcep.machinenodes.helper.actors.{ACK, CreateRemoteOperator, RemoteOperatorCreated}
import tcep.simulation.adaptive.cep.SystemLoad
import tcep.utils.{SizeEstimator, TransitionLogPublisher}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * Created by mac on 09/08/2017.
  */
trait ClusterActor extends Actor with ActorLogging {
  val cluster = Cluster(context.system)
  implicit val ec: ExecutionContext = context.system.dispatcher
  lazy val blockingIoDispatcher: ExecutionContext = cluster.system.dispatchers.lookup("blocking-io-dispatcher")
  var currentLoad: Double = 0.0
  var updateLoadTick: Cancellable = _
  val retryTimeout = Timeout(ConfigFactory.load().getLong("constants.retry-timeout"), TimeUnit.SECONDS)
  val retries = ConfigFactory.load().getInt("constants.default-retries")
  var transitionLogPublisher: ActorRef = context.actorOf(Props[TransitionLogPublisher], s"TransitionLogPublisher")
  val debugTransitionsOnly: Boolean = ConfigFactory.load().getBoolean("constants.mapek.enable-distributed-transition-debugging")
  val transitionLogEnabled: Boolean = debugTransitionsOnly || log.isDebugEnabled
  def transitionLog(msg: String, logPublisher: ActorRef = transitionLogPublisher): Unit = {
    if(transitionLogEnabled){
      log.debug(s"$self: $msg")
      logPublisher ! msg
    }
  }
  // compute placeholder value just once since SizeEstimator call is expensive during execution
  val ackSize: Long = SizeEstimator.estimate(ACK())
  def transitionRequestSize(child: ActorRef): Long =
    if(child.path.address != cluster.selfAddress) SizeEstimator.estimate(TransitionRequest(_, _, _)) + ackSize
    else 0
  def transferredStateSize(parent: ActorRef): Long =
    if(parent.path.address != cluster.selfAddress) (SizeEstimator.estimate(TransferredState(_, _, _, _)) + ackSize)
    else 0
  def subUnsubOverhead(successor: ActorRef, parents: List[ActorRef]): Long =
    parents.count(_.path.address != successor.path.address) * (SizeEstimator.estimate(Subscribe(_)) + ackSize) +
      parents.count(_.path.address != cluster.selfAddress) * SizeEstimator.estimate(UnSubscribe()) // subscribe, ack, unsubscribe
  def remoteOperatorCreationOverhead(successor: ActorRef): Long =
    if(successor.path.address != cluster.selfAddress) SizeEstimator.estimate(CreateRemoteOperator(_,_)) + SizeEstimator.estimate(RemoteOperatorCreated(_))
    else 0

  /**
    * update the placement overhead and transition overhead fields of TransitionStats
    * only add the transition overhead if the message came from another host (i.e. was transmitted over the network)
    */
  def updateTransitionStats(stats: TransitionStats, transitionMsgSender: ActorRef, transitionMsgSize: Long = 0, placementMsgSize: Long = 0, updatedOpMap: Option[Map[ActorRef, Long]] = None): TransitionStats = {
    if(transitionMsgSender.path.address.host.isDefined && transitionMsgSender.path.address != cluster.selfAddress) {
      TransitionStats(stats.placementOverheadBytes + placementMsgSize, stats.transitionOverheadBytes + transitionMsgSize, stats.transitionStartAtKnowledge, updatedOpMap.getOrElse(stats.transitionTimesPerOperator))
    } else {
      TransitionStats(stats.placementOverheadBytes + placementMsgSize, stats.transitionOverheadBytes, stats.transitionStartAtKnowledge, updatedOpMap.getOrElse(stats.transitionTimesPerOperator))
    }
  }

  override def preStart(): Unit = {
    super.preStart()
    cluster.subscribe(self, classOf[MemberEvent])
    updateLoadTick = context.system.scheduler.scheduleWithFixedDelay(FiniteDuration(1, TimeUnit.SECONDS), FiniteDuration(1, TimeUnit.SECONDS))(() => {
      currentLoad = SystemLoad.getSystemLoad
    })(blockingIoDispatcher)
  }

  override def postStop(): Unit = {
    transitionLogPublisher ! PoisonPill
    updateLoadTick.cancel()
    super.postStop()
    cluster.unsubscribe(self)
  }
}
