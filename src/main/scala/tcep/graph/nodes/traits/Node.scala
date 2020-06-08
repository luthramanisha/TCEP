package tcep.graph.nodes.traits

import java.time.Instant

import akka.actor.{ActorLogging, ActorRef, Address, Cancellable, PoisonPill, Props}
import com.typesafe.config.ConfigFactory
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.{Dependencies, OperatorMigrationNotice, UnSubscribe, UpdateTask}
import tcep.graph.nodes.traits.TransitionExecutionModes.ExecutionMode
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.transition.{StartExecution, StartExecutionAtTime, StartExecutionWithData, TransitionStats}
import tcep.graph.{CreatedCallback, EventCallback}
import tcep.machinenodes.helper.actors.{CreateRemoteOperator, PlacementMessage, RemoteOperatorCreated}
import tcep.placement._
import tcep.placement.manets.StarksAlgorithm
import tcep.placement.mop.RizouAlgorithm
import tcep.placement.sbon.PietzuchAlgorithm
import tcep.publishers.Publisher.AcknowledgeSubscription
import tcep.simulation.adaptive.cep.SystemLoad
import tcep.simulation.tcep.GUIConnector
import tcep.utils.{SpecialStats, TCEPUtils}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * The base class for all of the nodes
  **/
trait Node extends MFGSMode with SMSMode with ActorLogging {

  val name: String = self.path.name
  val hostInfo: HostInfo
  val query: Query
  val transitionConfig: TransitionConfig
  @volatile var started: Boolean = false
  val placementUpdateInterval: Int = ConfigFactory.load().getInt("constants.placement.update-interval")
  val eventIntervalMicros: Int = ConfigFactory.load().getInt("constants.event-interval-microseconds")
  val backupMode: Boolean
  val mainNode: Option[ActorRef]
  private var currAlgorithm: String = ""
  implicit val creatorAddress: Address = cluster.selfAddress

  val subscribers: mutable.Set[ActorRef] = mutable.Set[ActorRef]()
  var bdpToParents: mutable.Map[Address, Double] = mutable.Map()
  val createdCallback: Option[CreatedCallback]
  val eventCallback: Option[EventCallback]

  var updateTask: Cancellable = _
  SystemLoad.newOperatorAdded()

  override def executeTransition(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): Unit = {
    log.info(s"${self.path.name} executing transition on $transitionConfig")
    if (transitionConfig.transitionStrategy == TransitionModeNames.SMS) super[SMSMode].executeTransition(requester, algorithm, stats)
    else super[MFGSMode].executeTransition(requester, algorithm, stats)

  }

  override def preStart(): X = {
    super.preStart()
    //executionStarted()
    // must explicitly start execution by receiving StartExecution(WithData/WithDependencies)
    // message from knowledge actor (initial) or predecessor (when transiting)
    scheduleHeartBeat()
    log.info(s"starting operator $self in transition mode $transitionConfig")
  }

  override def postStop(): Unit = {
    getParentOperators().foreach(_ ! UnSubscribe())
    log.info(s"unsubscribed from parents and stopped self $self")
    super.postStop()
  }

  def scheduleHeartBeat() = {
    if (backupMode) {
      started = false
      this.context.watch(mainNode.get)
    }
  }

  def childNodeReceive: Receive

  override def receive: Receive = placementReceive orElse (
      if (transitionConfig.transitionStrategy == TransitionModeNames.SMS)
        super[SMSMode].transitionReceive
      else
        super[MFGSMode].transitionReceive
      ) andThen childNodeReceive

  def placementReceive: Receive = {

    case AcknowledgeSubscription() if getParentOperators.contains(sender()) => log.debug(s"received subscription ACK from ${sender()}")
    case StartExecution(algorithm) => {
      if(!started) {
        started = true
        subscribeToParents()
        self ! UpdateTask(algorithm)
        // during tests, this message is sent only to the root and then forwarded to parents to ensure sequential start and ordered receive of Created msg;
        // during normal operation, the message is broadcast to all operators at once since the Created msg is not of importance
        getParentOperators.foreach(_ ! StartExecution(algorithm))
        log.info(s"started operator $this $started")
      }
    }
    // placement update task that must be set/sent by the actor deploying this operator
    // 1. receive notification after initial deployment, setting up a task on each operator
    // 2. periodically execute placement algorithm locally, migrate to new node (see transition) if necessary
    // 3. notify dependencies of new location
    case UpdateTask(algorithmType: String) => {
      currAlgorithm = algorithmType
      val algorithm = algorithmType match {
        case PietzuchAlgorithm.name => PietzuchAlgorithm
        case RizouAlgorithm.name => RizouAlgorithm
        case StarksAlgorithm.name => StarksAlgorithm
        case RandomAlgorithm.name => RandomAlgorithm
        case MobilityTolerantAlgorithm.name => MobilityTolerantAlgorithm
        case GlobalOptimalBDPAlgorithm.name => GlobalOptimalBDPAlgorithm
        case other: String => throw new NoSuchElementException(s"need to add algorithm type $other to updateTask!")
      }
      if (algorithm.hasPeriodicUpdate()) {

        val task: Runnable = () => {
          implicit val ec: ExecutionContext = blockingIoDispatcher
          val startTime = System.currentTimeMillis()
          val dependencies = Dependencies(getParentOperators.toList, subscribers.toList)
          for {
            init <- algorithm.initialize(cluster)
            optimalHost: HostInfo <- algorithm.findOptimalNode(this.context, this.cluster, dependencies, HostInfo(cluster.selfMember, hostInfo.operator), hostInfo.operator)
          } yield {
            if (!cluster.selfAddress.equals(optimalHost.member.address)) {
              log.info(s"Relaxation periodic placement update for $query: " +
                s"\n moving operator from ${cluster.selfMember} to ${optimalHost.member} " +
                s"\n with dependencies $dependencies")
              // creates copy of operator on new host
              for { newOperator: ActorRef <- createDuplicateNode(optimalHost) } yield {

              this.started = false
              val downTime = System.currentTimeMillis()
              if (transitionConfig.transitionStrategy == TransitionModeNames.MFGS) newOperator ! StartExecutionWithData(downTime, startTime, subscribers.toSet, slidingMessageQueue.toSet, algorithmType)
              else newOperator ! StartExecutionAtTime(subscribers.toList, Instant.now(), algorithmType)
              newOperator ! UpdateTask(algorithmType)
              // inform subscribers about new operator location; parent will receive
              subscribers.foreach(_ ! OperatorMigrationNotice(self, newOperator))

              // taken from MFGSMode
              // TODO maybe make regular operator migrations distinguishable from transitions
              val timestamp = System.currentTimeMillis()
              val migrationTime = timestamp - downTime
              val nodeSelectionTime = timestamp - startTime
              GUIConnector.sendOperatorTransitionUpdate(self, newOperator, algorithm.name, timestamp, migrationTime, nodeSelectionTime, getParentOperators(), optimalHost, isRootOperator)(selfAddress = cluster.selfAddress)

              log.info(s"${self} shutting down Self")
              SystemLoad.operatorRemoved()
              updateTask.cancel()
              self ! PoisonPill

            }} else log.info(s"periodic placement update for $query: no change of host \n dependencies: ${dependencies}")
          }
        }

        updateTask = context.system.scheduler.schedule(placementUpdateInterval seconds, placementUpdateInterval seconds, task)
        log.info(s"received placement update task, setting up periodic placement update every ${placementUpdateInterval}s")
      }
    }
  }

  def subscribeToParents()

  def emitCreated(): Unit = {
    if (createdCallback.isDefined) {
      log.info("applying created callback".toUpperCase())
      createdCallback.get.apply()
    } else {
      log.info(s"no callback, sending Created to subscribers $subscribers".toUpperCase())
      subscribers.foreach(_ ! Created)
    }
  }

  def emitEvent(event: Event): Unit = {
    if (started) {
      val s = sender()
      //else discarding event
      updateMonitoringData(log, event, hostInfo, currentLoad)
      subscribers.foreach(sub => {
        //SpecialStats.log(s"$this", s"sendEvent_${currAlgorithm}_${self.path.name}", s"STREAMING EVENT $event FROM ${s} TO ${sub}")
        //log.debug(s"STREAMING EVENT $event FROM ${s.path.address.host} TO ${sub.path.address.host}")
        if (eventCallback.isDefined) {
          log.debug(s"applying callback to event $event")
          eventCallback.get.apply(event)
        }
        sub ! event
      })
    } else {
      log.info(s"started $started, discarding event $event from ${sender} $getParentOperators")
    }
  }

  def createDuplicateNode(nodeInfo: HostInfo): Future[ActorRef] = {
    val startTime = System.currentTimeMillis()
    val props = Props(getClass, transitionConfig, nodeInfo, backupMode, mainNode, query, createdCallback, eventCallback, isRootOperator, getParentOperators)
    for {
      taskManager <- TCEPUtils.getTaskManagerOfMember(cluster, nodeInfo.member)
      deployDuplicate <- TCEPUtils.guaranteedDelivery(context, taskManager, CreateRemoteOperator(nodeInfo, props), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher)).mapTo[RemoteOperatorCreated]
    } yield {
      log.info(s"$self spent ${System.currentTimeMillis() - startTime} milliseconds to create duplicate ${deployDuplicate.ref}")
      deployDuplicate.ref
    }
  }


  def toAnyRef(any: Any): AnyRef = {
    // Yep, an `AnyVal` can safely be cast to `AnyRef`:
    // https://stackoverflow.com/questions/25931611/why-anyval-can-be-converted-into-anyref-at-run-time-in-scala
    any.asInstanceOf[AnyRef]
  }

}



object Node {
  case class Subscribe(subscriber: ActorRef) extends PlacementMessage
  case class UnSubscribe() extends PlacementMessage
  case class UpdateTask(algorithmType: String) extends PlacementMessage
  case class Dependencies(parents: List[ActorRef], subscribers: List[ActorRef])
  case class OperatorMigrationNotice(oldOperator: ActorRef, newOperator: ActorRef) extends PlacementMessage
}

case class TransitionConfig(
                             transitionStrategy: Mode = TransitionModeNames.MFGS,
                             transitionExecutionMode: ExecutionMode = TransitionExecutionModes.CONCURRENT_MODE)
object TransitionExecutionModes extends Enumeration {
  type ExecutionMode = Value
  val CONCURRENT_MODE, SEQUENTIAL_MODE = Value
}

object TransitionModeNames extends Enumeration {
  type Mode = Value
  val SMS, MFGS = Value
}
