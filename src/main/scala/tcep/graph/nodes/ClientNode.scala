package tcep.graph.nodes

import akka.actor.{ActorLogging, ActorRef, PoisonPill}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.DistVivaldiActor
import tcep.ClusterActor
import tcep.data.Events
import tcep.data.Events._
import tcep.graph.nodes.traits.Node.{OperatorMigrationNotice, Subscribe}
import tcep.graph.transition.MAPEK.{SetLastTransitionStats, SetTransitionStatus, UpdateLatency}
import tcep.graph.transition.{MAPEK, TransferredState, TransitionRequest}
import tcep.machinenodes.consumers.Consumer.SetStatus
import tcep.machinenodes.helper.actors.{ACK, PlacementMessage}
import tcep.placement.{HostInfo, OperatorMetrics}
import tcep.simulation.tcep.GUIConnector
import tcep.utils.{SpecialStats, TCEPUtils}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Receives the final output of the query
  *
  **/

class ClientNode(var rootOperator: ActorRef, /*monitorFactories: Array[MonitorFactory], */mapek: MAPEK, var consumer: ActorRef = null) extends ClusterActor with ActorLogging {

  log.info("Creating Clientnode")
  val defaultBandwidth = ConfigFactory.load().getInt("constants.default-data-rate")
  //val monitors = monitorFactories.map(f => f.createNodeMonitor)
  var hostInfo: HostInfo = _
  var transitionStartTime: Long = _   //Not in transition
  var transitionStatus: Int = 0
  var guiBDPUpdateSent = false
  implicit val timeout = Timeout(5 seconds)


  override def receive: Receive = {

    case TransitionRequest(strategy, requester, stats) => {
      sender() ! ACK()
      if(transitionStatus == 0) {
        log.info(s"Transiting system to ${strategy.name}")
        transitionLog(s"transiting to ${strategy.name}")
        mapek.knowledge ! SetTransitionStatus(1)
        transitionStartTime = System.currentTimeMillis()
        TCEPUtils.guaranteedDelivery(context, rootOperator, TransitionRequest(strategy, self, stats), tlf = Some(transitionLog), tlp = Some(transitionLogPublisher))(blockingIoDispatcher)
      }
    }

    case TransferredState(_, replacement, oldParent, stats) => {
      sender() ! ACK()
      // log transition time of parent
      val transitionStart: Long = stats.transitionTimesPerOperator.getOrElse(oldParent, 0)
      val transitionDuration = System.currentTimeMillis() - transitionStart
      val opmap = stats.transitionTimesPerOperator.updated(oldParent, transitionDuration)
      log.info(s"parent $oldParent \n transition start: $transitionStart, transitionDuration: ${transitionDuration} , operator stats: \n ${stats.transitionTimesPerOperator.mkString("\n")}")
      val updatedStats =  updateTransitionStats(stats, oldParent, transferredStateSize(oldParent), updatedOpMap = Some(opmap) )
      mapek.knowledge ! SetTransitionStatus(0)
      mapek.knowledge ! SetLastTransitionStats(updatedStats)
      if(transitionStatus != 0) {
        val timetaken = System.currentTimeMillis() - transitionStartTime
        log.info(s"replacing operator node after $timetaken ms \n $rootOperator \n with replacement $replacement")
        replaceOperator(replacement)
        transitionLog(s"transition complete after $timetaken ms (from $oldParent to $replacement \n\n")
        GUIConnector.sendTransitionTimeUpdate(timetaken.toDouble / 1000)(blockingIoDispatcher)
      }
    }

    case OperatorMigrationNotice(oldOperator, newOperator) => { // received from migrating parent (oldOperator)
      this.replaceOperator(newOperator)
      log.info(s"received operator migration notice from $oldOperator, \n new operator is $newOperator")
    }

    case event: Event if sender().equals(rootOperator) && hostInfo != null => {
      val s = sender()
      //Events.printEvent(event, log)
      //val arrival = System.nanoTime()
      Events.updateMonitoringData(log, event, hostInfo, currentLoad) // also updates hostInfo.operatorMetrics.accumulatedBDP
      if(!guiBDPUpdateSent) {
        GUIConnector.sendBDPUpdate(hostInfo.graphTotalBDP, DistVivaldiActor.getLatencyValues())(cluster.selfAddress, blockingIoDispatcher) // this is the total BDP of the entire graph
        guiBDPUpdateSent = true
        SpecialStats.debug(s"$this", s"hostInfo after update: ${hostInfo.operatorMetrics}")
      }
      consumer ! event
      //monitors.foreach(monitor => monitor.onEventEmit(event, transitionStatus))
      //val now = System.nanoTime()
      val e2eLatency = System.currentTimeMillis() - event.monitoringData.creationTimestamp
      mapek.knowledge ! UpdateLatency(e2eLatency)
      //SpecialStats.log(s"$this", "clientNodeEvents", s"received event $event from $s;
      // ${event.monitoringData.lastUpdate.map(e => e._1 -> (System.currentTimeMillis() - e._2) ).mkString(";")};
      // e2e latency: ${e2eLatency}ms; time since arrival: ${(now - arrival) / 1e6}ms;
      // event size: ${SizeEstimator.estimate(event)}")
    }

    case event: Event if !sender().equals(rootOperator) =>
      log.info(s"unknown sender ${sender().path.name} my parent is ${rootOperator.path.name}")

    case SetTransitionStatus(status) =>
      this.transitionStatus = status
      this.consumer ! SetStatus(status)

    case ShutDown() => {
      rootOperator ! ShutDown()
      log.info(s"Stopping self as received shut down message from ${sender().path.name}, forwarding it to $rootOperator")
      self ! PoisonPill
      //this.consumer ! PoisonPill
    }

    case _ =>
  }

  /**
    * replace parent operator reference so that new events are recognized correctly
    * @param replacement replacement ActorRef
    * @return
    */
  private def replaceOperator(replacement: ActorRef) = {
    for {bdpToOperator: Double <- this.getBDPToOperator(replacement)} yield {
      hostInfo = HostInfo(this.cluster.selfMember, null, OperatorMetrics(Map(rootOperator.path.name -> bdpToOperator))) // the client node does not have its own operator, hence null
      this.rootOperator = replacement
      guiBDPUpdateSent = false // total (accumulated) bdp of the entire operator graph is updated when the first event arrives
    }
  }

  def getBDPToOperator(operator: ActorRef): Future[Double] = {
    val operatorMember = TCEPUtils.getMemberFromActorRef(cluster, operator)
    def bdpRequest = TCEPUtils.getBDPBetweenNodes(cluster, cluster.selfMember, operatorMember)(blockingIoDispatcher)
    bdpRequest recoverWith {
      case e: Throwable =>
        log.warning(s"failed to get BDP between clientNode and root operator, retrying once... \n cause: $e")
        bdpRequest
    } recoverWith { case _ => TCEPUtils.getVivaldiDistance(cluster, cluster.selfMember, operatorMember)(blockingIoDispatcher) map { _ * defaultBandwidth }
    }
  }

  override def preStart(): Unit = {
    super.preStart()
    for { bdpToOperator <- this.getBDPToOperator(rootOperator) } yield {
      hostInfo = HostInfo(this.cluster.selfMember, null, OperatorMetrics(Map(rootOperator.path.name -> bdpToOperator))) // the client node does not have its own operator, hence null
      log.info(s"Subscribing for events from ${rootOperator.path.name}")
      rootOperator ! Subscribe(self)
    }
  }

  override def postStop(): Unit = {
    super.postStop()
    log.info("Stopping self")
  }

}

case class ShutDown() extends PlacementMessage