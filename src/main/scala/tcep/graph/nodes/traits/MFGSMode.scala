package tcep.graph.nodes.traits

import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{ActorRef, PoisonPill}
import akka.cluster.Cluster
import akka.dispatch.Dispatchers
import tcep.data.Events.Event
import tcep.graph.nodes.StreamNode
import tcep.graph.nodes.traits.Node.{Dependencies, UnSubscribe, UpdateTask}
import tcep.graph.transition._
import tcep.machinenodes.helper.actors.ACK
import tcep.placement.{HostInfo, OperatorMetrics, PlacementStrategy}
import tcep.simulation.adaptive.cep.SystemLoad
import tcep.simulation.tcep.GUIConnector
import tcep.utils.{SizeEstimator, SpecialStats, TCEPUtils}
import tcep.utils.TransitionLogActor.{OperatorTransitionBegin, OperatorTransitionEnd}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.parsing.json.JSONObject
import scala.util.{Failure, Success}

/**
  * Handling Cases of MFGS Transition
  **/
trait MFGSMode extends TransitionMode {
  override val modeName = "MFGS mode"

  val slidingMessageQueue = {
    ListBuffer[(ActorRef, Any)]()
  }

  override def preStart(): Unit = {
    super.preStart()
    messageQueueManager()
  }

  def messageQueueManager() = {
    if (!maxWindowTime().isZero) {
      context.system.scheduler.schedule(
        FiniteDuration(maxWindowTime().getSeconds, TimeUnit.SECONDS),
        FiniteDuration(maxWindowTime().getSeconds, TimeUnit.SECONDS), () => {
          slidingMessageQueue.clear()
        }
      )
    }
  }

  override def transitionReceive = this.mfgsReceive orElse super.transitionReceive

  private def mfgsReceive: PartialFunction[Any, Any] = {


    case StartExecutionWithData(downtime, t, subs, data, algorithm) => {
      sender() ! ACK()
      transitionLog(s"received StartExecutionWithData from predecessor $sender")
      if(!this.started) {
        log.debug("data (items: ${data.size}): ${data} \n subs : ${subs}")
        transitionLog(s"received StartExecutionWithData (downtime: ${System.currentTimeMillis() - downtime}ms) from old node ${sender()}")
        this.started = true
        this.subscribers ++= subs
        data.foreach(m => self.!(m._2)(m._1))
        subscribeToParents()
        self ! UpdateTask(algorithm)
        //Unit //Nothing to forward for childReceive
      }
    }

    case message: Event => {
      if(!maxWindowTime().isZero) slidingMessageQueue += Tuple2(sender(), message) // check window time to avoid storing all incoming events without ever clearing them -> memory problem
      message //caching message and forwarding to the childReceive
    }

  }

  override def executeTransition(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): Unit = {

    try {
      transitionLog(s"executing $modeName transition to ${algorithm.name} requested by ${requester}")
      transitionLogPublisher ! OperatorTransitionBegin(self)
      val startTime = System.currentTimeMillis()
      var downTime: Option[Long] = None
      val cluster = Cluster(context.system)
      @volatile var succHostFound: Option[HostInfo] = None
      @volatile var succStarted = false
      @volatile var childNotified = false
      implicit val timeout = retryTimeout
      implicit val ec: ExecutionContext = blockingIoDispatcher

      val parents: List[ActorRef] = getParentOperators.toList
      val dependencies = Dependencies(parents, subscribers.toList)

      // helper functions for retrying intermediate steps upon failure
      def findSuccessorHost(): Future[HostInfo] = {
        if (succHostFound.isDefined) {
          log.debug(s"successor host was found before, returning it: ${succHostFound.get.member}")
          Future { succHostFound.get }
        } else {
          log.debug("findSuccessorHost() called for the first time")
          val req = for {
            wasInitialized <- algorithm.initialize(cluster, caller = Some(self))
            successorHost: HostInfo <- {
              transitionLog(s"initialized algorithm $algorithm (was initialized: $wasInitialized), looking for new host..."); algorithm.findOptimalNode(this.context, cluster, dependencies, HostInfo(cluster.selfMember, hostInfo.operator, OperatorMetrics()), hostInfo.operator)
            }
          } yield {
            transitionLog(s"found new host ${successorHost.member.address}")
            succHostFound = Some(successorHost)
            successorHost
          }
          req.recoverWith { case e: Throwable => transitionLog(s"failed to find successor host, retrying... ${e.toString}"); findSuccessorHost() }
        }
      }

      def startSuccessorActor(successor: ActorRef, newHostInfo: HostInfo): Future[TransitionStats] = {
        if(succStarted) return Future { stats } // avoid re-sending loop
        this.started = false
        if(downTime.isEmpty) downTime = Some(System.currentTimeMillis())
        // resend incoming messages to self on new operator if windowed so we don't lose any events
        val msgQueue = if (!this.maxWindowTime().isZero) slidingMessageQueue.toSet else Set[(ActorRef, Any)]() // use toSet to avoid re-sending duplicates
        log.info(s"sending StartExecutionWithData to $successor with timeout $retryTimeout")
        val startExecutionMessage = StartExecutionWithData(downTime.get, startTime, subscribers.toSet, msgQueue, algorithm.name)
        val msgACK = TCEPUtils.guaranteedDelivery(context, successor, startExecutionMessage, tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
        msgACK.map(ack => {
          succStarted = true
          val timestamp = System.currentTimeMillis()
          val migrationTime = timestamp - downTime.get
          val nodeSelectionTime = timestamp - startTime
          GUIConnector.sendOperatorTransitionUpdate(self, successor, algorithm.name, timestamp, migrationTime, nodeSelectionTime, parents, newHostInfo, isRootOperator)(cluster.selfAddress)
          notifyMAPEK(cluster, successor) // notify mapek knowledge about operator change
          transitionLog(s"received ACK for StartExecutionWithData from successor ${System.currentTimeMillis() - downTime.get}ms after stopping events (total time: $migrationTime ms), handing control over to requester and shutting down self")
          val placementOverhead = newHostInfo.operatorMetrics.accPlacementMsgOverhead
          // remote operator creation, state transfer and execution start, subscription management
          val transitionOverhead = remoteOperatorCreationOverhead(successor) + SizeEstimator.estimate(startExecutionMessage) + ackSize + subUnsubOverhead(successor, parents)
          TransitionStats(stats.placementOverheadBytes + placementOverhead, stats.transitionOverheadBytes + transitionOverhead, stats.transitionStartAtKnowledge, stats.transitionTimesPerOperator)
        }).mapTo[TransitionStats].recoverWith { case e: Throwable => transitionLog(s"failed to start successor actor (started: $succStarted, retrying... ${e.toString}".toUpperCase()); startSuccessorActor(successor, newHostInfo) }
      }

      def notifyChild(successor: ActorRef, updatedStats: TransitionStats): Future[ACK] = {
        if (childNotified) return Future { ACK() }
        val msgACK = TCEPUtils.guaranteedDelivery(context, requester, TransferredState(algorithm, successor, self, updatedStats), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
          .map(ack => { childNotified = true; ack }).recoverWith { case e: Throwable => transitionLog(s"failed to notify child retrying... ${e.toString}".toUpperCase()); notifyChild(successor, updatedStats) }
        msgACK
      }.mapTo[ACK]

      transitionLog( s"MFGS transition: looking for new host of ${self} dependencies: $dependencies")
      // retry indefinitely on failure of intermediate steps without repeating the previous ones (-> no duplicate operators)
      val transitToSuccessor = for {
        host: HostInfo <- findSuccessorHost()
        successor: ActorRef <- { val s = createDuplicateNode(host); transitionLog(s"created successor duplicate ${s} on new host after ${System.currentTimeMillis() - startTime}ms, stopping events and sending StartExecutionWithData"); s }
        updatedStats <- startSuccessorActor(successor, host)
        childNotified <- notifyChild(successor, updatedStats)
      } yield {
        SystemLoad.operatorRemoved()
        parents.foreach(_ ! UnSubscribe())
        transitionLogPublisher ! OperatorTransitionEnd(self)
        sendTransitionStats(self, System.currentTimeMillis() - startTime, updatedStats.placementOverheadBytes, updatedStats.transitionOverheadBytes)
        context.system.scheduler.scheduleOnce(FiniteDuration(10, TimeUnit.SECONDS), self, PoisonPill) // delay self shutdown so that transferredState msg etc can be safely transmitted
      }

      transitToSuccessor.onComplete {
        case Success(_) =>
          transitionLog(s"successfully started successor operator and handed control back to requesting child ${requester.path}")
          log.info(s"transition of $this took ${System.currentTimeMillis() - startTime}ms")
        case Failure(exception) =>
          log.error(exception, s"failed to find new host for $this, see placement and transition logs")
          transitionLog(s"failed to transit to successor due to $exception")
      }

    } catch {
      case e: Throwable =>
        transitionLog(s"failed to find new host due to ${e.toString}")
        log.error(e, s"caught exception while executing transition for $this")
    }
  }

  def subscribeToParents()

}