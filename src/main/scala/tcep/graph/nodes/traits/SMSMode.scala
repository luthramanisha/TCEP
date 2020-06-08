package tcep.graph.nodes.traits

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{ActorRef, PoisonPill}
import akka.cluster.{Cluster, Member}
import tcep.data.Events.Event
import tcep.graph.nodes.traits.Node.{Dependencies, UnSubscribe, UpdateTask}
import tcep.graph.transition.{StartExecutionAtTime, TransferredState, TransitionStats}
import tcep.machinenodes.helper.actors.ACK
import tcep.placement.{HostInfo, OperatorMetrics, PlacementStrategy}
import tcep.simulation.adaptive.cep.SystemLoad
import tcep.simulation.tcep.GUIConnector
import tcep.utils.TransitionLogActor.{OperatorTransitionBegin, OperatorTransitionEnd}
import tcep.utils.{SizeEstimator, SpecialStats, TCEPUtils}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


/**
  * Handling cases of SMS Transition
  **/
trait SMSMode extends TransitionMode {
  override val modeName = "SMS mode"
  protected val deltaFactor = 10.0
  private var sentSubscribe = false
  val sched = Executors.newSingleThreadScheduledExecutor()
  val task = new Runnable {
    def run() = {
      startEvents()
    }
  }
  private def startEvents(): Unit = {
    this.started = true
    transitionLog(s"it is ${Instant.now()}, predecessor should stop right now, starting to stream events to $subscribers")

  }

  override def transitionReceive = this.smsReceive orElse super.transitionReceive

  private def smsReceive: PartialFunction[Any, Any] = {

    case StartExecutionAtTime(subs, minStateTime, algorithm) =>
      sender() ! ACK()
      transitionLog(s"received StartExecutionAtTime from predecessor $sender")
      if(!sentSubscribe) {
        sentSubscribe = true
        subscribeToParents()
        this.subscribers ++= subs
        self ! UpdateTask(algorithm)
        // take into account the time passed between determining minStateTime and receiving the ACK
        if (Instant.now().isAfter(minStateTime)) { // msg took longer than allocated time window -> start events immediately
          transitionLog(s"received StartExecutionAtTime from predecessor, it is AFTER minStateTime $minStateTime (${minStateTime.until(Instant.now(), ChronoUnit.MILLIS)}ms too late)-> starting events immediately")
          startEvents()
        } else { // still time left until minStateTime, schedule event send start
          val timeLeftUntil = Instant.now().until(minStateTime, ChronoUnit.NANOS)
          sched.schedule(task, timeLeftUntil, TimeUnit.NANOSECONDS)
          transitionLog(s"received StartExecutionAtTime from predecessor, it is BEFORE minStateTime $minStateTime -> scheduling event start in ${timeLeftUntil / 1e6}ms")
        }
        self ! UpdateTask(algorithm)
      }

    case message: Event if started =>
      message //forwarding to the childReceive (via smsReceive andThen childReceive)
  }

  override def executeTransition(requester: ActorRef, algorithm: PlacementStrategy, stats: TransitionStats): Unit = {
      try {
        transitionLog(s"executing $modeName transition to ${algorithm.name} requested by ${requester}")
        transitionLogPublisher ! OperatorTransitionBegin(self)
        val startTime = System.currentTimeMillis()
        //val updatedStats = updateTransitionStats(stats, self, stats.transitionOverheadBytes, stats.placementOverheadBytes, Some(stats.transitionTimesPerOperator.updated(self, System.currentTimeMillis())))
        val cluster = Cluster(context.system)
        @volatile var succHostFound: Option[HostInfo] = None
        @volatile var succStarted = false
        @volatile var childNotified = false
        implicit val timeout = retryTimeout
        implicit val ec: ExecutionContext = blockingIoDispatcher
        val parents: List[ActorRef] = getParentOperators.toList
        val dependencies = Dependencies(parents, subscribers.toList)
        var delayToSuccessor: Long = 0
        var delayToParent = System.currentTimeMillis() - stats.transitionEndParent

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
                transitionLog(s"initialized algorithm $algorithm (was initialized: $wasInitialized), looking for new host...");
                algorithm.findOptimalNode(this.context, cluster, dependencies, HostInfo(cluster.selfMember, hostInfo.operator, OperatorMetrics()), hostInfo.operator)
              }
            } yield {
              transitionLog(s"found new host ${successorHost.member.address}")
              succHostFound = Some(successorHost)
              successorHost
            }
            req.recoverWith { case e: Throwable => transitionLog(s"failed to find successor host, retrying... ${e.toString}"); findSuccessorHost() }
          }
        }

        def startSuccessorActor(successor: ActorRef, newHostInfo: HostInfo): Future[Unit] = {
          if (succStarted) return Future {} // avoid re-sending loop

          // determine time when to stop event processing of this operator on this host (old) and start it on new host
          // vivaldi coord distance sometimes very large (>5s)
          for {
            vivDistToSuccessorHost <- TCEPUtils.getVivaldiDistance(cluster, cluster.selfMember, newHostInfo.member)
            vivDistToParents <- Future.sequence(getParentOperators().map(p => TCEPUtils.getMemberFromActorRef(cluster, p)).map(p => TCEPUtils.getVivaldiDistance(cluster, newHostInfo.member, p)))
            delta = Duration.ofMillis((deltaFactor * (2 * vivDistToSuccessorHost + 2 * vivDistToParents.max)).toLong) // transmission times: startMsg + ack + subscribe + ack; multiply by factor to allow for vivaldi inaccuracies
          } yield {

            //val delta = Duration.ofMillis((deltaFactor * 2 * (delayToParent + delayToSuccessor)).toLong)
            SpecialStats.log("SMS", "SMS-delta", s"delta: $delta, delayto successor: $delayToSuccessor toparent $delayToParent")
            val minStateTimeDelay: Duration = // delta: allow duplicate operator to subscribe to event streams (so it can immediately send when the old operator stops at minStateTime)
              if (!maxWindowTime().isZero) maxWindowTime().plus(delta) // windowed operators: delay the operator start on new host according to window size to wait for an entire event window to be processed
              else delta
            val minStateReference = Instant.now()
            val minStateTime: Instant = minStateReference.plus(minStateTimeDelay)

            log.info(s"sending StartExecutionAtTime with time $minStateTime (delta: $delta) to $successor")
            val startExecutionMessage = StartExecutionAtTime(subscribers.toList, minStateTime, algorithm.name)
            val msgACK = TCEPUtils.guaranteedDelivery(context, successor, startExecutionMessage, tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
            msgACK.map(ack => {
              succStarted = true
              val timestamp = System.currentTimeMillis()
              val migrationTime = 0
              val nodeSelectionTime = timestamp - startTime
              GUIConnector.sendOperatorTransitionUpdate(self, successor, algorithm.name, timestamp, migrationTime, nodeSelectionTime, parents, newHostInfo, isRootOperator)(cluster.selfAddress)
              notifyMAPEK(cluster, successor) // notify mapek knowledge about operator change
              val placementOverhead = newHostInfo.operatorMetrics.accPlacementMsgOverhead
              // remote operator creation, state transfer and execution start, subscription management
              val transitionOverhead = remoteOperatorCreationOverhead(successor) + SizeEstimator.estimate(startExecutionMessage) + ackSize + subUnsubOverhead(successor, parents)
              val updatedStats = TransitionStats(stats.placementOverheadBytes + placementOverhead, stats.transitionOverheadBytes + transitionOverhead, stats.transitionStartAtKnowledge, stats.transitionTimesPerOperator)
              // take into account the time passed between determining minStateTime and receiving the ACK
              if (Instant.now().isAfter(minStateTime)) { // ACK took longer than allocated time window -> stop events immediately
                transitionLog(s"received ACK for StartExecutionAtTime from successor, it is AFTER minStateTime $minStateTime (${minStateTime.until(Instant.now(), ChronoUnit.MILLIS)}ms too late), -> stopping events immediately and notifying child")
                notifyChild(successor, updatedStats)
              } else { // still time left until minStateTime, schedule call to notifyChild
                val timeLeftUntil = Instant.now().until(minStateTime, ChronoUnit.NANOS)
                sched.schedule(() => notifyChild(successor, updatedStats), timeLeftUntil, TimeUnit.NANOSECONDS)
                transitionLog(s"received ACK for StartExecutionAtTime from successor, it is BEFORE minStateTime -> scheduling event stop and child notification in ${timeLeftUntil / 1e6}ms")
              }
              Unit
            }).mapTo[Unit].recoverWith { case e: Throwable => transitionLog(s"failed to start successor actor (started: $succStarted, retrying... ${e.toString}".toUpperCase()); startSuccessorActor(successor, newHostInfo) }
          }
        }

        // stop sending events send TransferredState msg to child so it can resume its own transition
        def notifyChild(successor: ActorRef, _stats: TransitionStats): Future[Unit] = {
          transitionLog("entered notifyChild")
          if (childNotified) return Future { ACK() }
          started = false
          val updatedStats = TransitionStats(_stats.placementOverheadBytes, _stats.transitionOverheadBytes,
            _stats.transitionStartAtKnowledge, _stats.transitionTimesPerOperator, transitionEndParent = System.currentTimeMillis()) // set end time of transition so child can use this timestamp
          transitionLog(s"transition stats map; ${updatedStats.transitionTimesPerOperator.mkString(";")}")

          transitionLog(s"stopped sending events at ${Instant.now()}, $successor should start sending at this moment; handing back control to child")
          val msgACK = TCEPUtils.guaranteedDelivery(context, requester, TransferredState(algorithm, successor, self, updatedStats), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))
            .map(ack => {
              childNotified = true;
              SystemLoad.operatorRemoved()
              parents.foreach(_ ! UnSubscribe())
              transitionLogPublisher ! OperatorTransitionEnd(self)
              sendTransitionStats(self, System.currentTimeMillis() - startTime, updatedStats.placementOverheadBytes, updatedStats.transitionOverheadBytes)
              context.system.scheduler.scheduleOnce(FiniteDuration(10, TimeUnit.SECONDS), self, PoisonPill) // delay self shutdown so that transferredState msg etc can be safely transmitted
              transitionLog(s"successfully started successor operator and handed control back to requesting child ${requester.path}")
              log.info(s"transition of $this took ${System.currentTimeMillis() - startTime}ms")
            }).recoverWith {
            case e: Throwable =>
              transitionLog(s"failed to notify child, retrying... ${e.toString}".toUpperCase());
              notifyChild(successor, updatedStats) }
          msgACK
        }.mapTo[Unit]

        // start transition with retry
        transitionLog( s"$modeName transition: looking for new host of ${self} dependencies: $dependencies")
        // retry indefinitely on failure of intermediate steps without repeating the previous ones (-> no duplicate operators)
        val transitToSuccessor = for {
          host: HostInfo <- findSuccessorHost()
          successor: ActorRef <- {
            val startCreate = System.currentTimeMillis()
            val s = createDuplicateNode(host);
            delayToSuccessor = System.currentTimeMillis() - startCreate
            transitionLog(s"created successor duplicate ${s} on new host after ${System.currentTimeMillis() - startTime}ms, stopping events and sending StartExecutionAtTime");
            s }
          updatedStats <- startSuccessorActor(successor, host)
          // last call is notifyChild, but this is scheduled to a specific time
        } yield Unit

        transitToSuccessor.onComplete {
          case Success(_) =>
          case Failure(exception) =>
            log.error(exception, s"failed to create and start successor for $this, see placement and transition logs")
            transitionLog(s"failed to create and start successor due to $exception")
        }

    } catch {
      case e: Throwable =>
        transitionLog(s"failed to find new host due to ${e.toString}")
        log.error(e, s"caught exception while executing transition for $this")
    }
  }

}