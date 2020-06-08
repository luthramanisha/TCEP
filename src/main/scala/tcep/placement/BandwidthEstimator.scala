package tcep.placement

import java.util.concurrent._

import akka.actor.ActorRef
import akka.cluster.Member
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import tcep.ClusterActor
import tcep.machinenodes.helper.actors._
import tcep.utils.{SpecialStats, TCEPUtils}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.sys.process._
import scala.util.{Failure, Success}

/**
  * @author sebastian
  * @author Niels
  */
class BandwidthEstimator() extends ClusterActor {

  val out = new StringBuilder
  val err = new StringBuilder

  val logger = ProcessLogger(
    (o: String) => out.append(o),
    (e: String) => err.append(e))

  //private val refreshInterval: Int = ConfigFactory.load().getInt("constants.data-rate-refresh-interval")
  //private val executor = new ScheduledThreadPoolExecutor(1)
  //private val task: Runnable = () => updateMeasurements
  //private val execution = executor.scheduleAtFixedRate(task, 0, refreshInterval, TimeUnit.SECONDS) // TODO disabled
  private val selfMeasurements: ListBuffer[DataRateMeasurement] = new ListBuffer[DataRateMeasurement]()
  private val allMeasurements: mutable.Map[(Member, Member), Double] = mutable Map[(Member, Member), Double]() // only used for mininet where bandwidth is fixed
  val defaultRate = ConfigFactory.load().getDouble("constants.default-data-rate")
  val mininetBandwidth = ConfigFactory.load().getDouble("constants.mininet-link-bandwidth")
  val isMininetSim: Boolean = ConfigFactory.load().getBoolean("constants.mininet-simulation")
  var initialized = false
  val measurementDuration = 5 // s

  var inboundMeasurementRequests: mutable.Queue[ActorRef] = mutable.Queue[ActorRef]() // inbound : requests from other nodes that want to contact this node's iperf server
  var inboundOngoingMeasurements: mutable.Queue[ActorRef] = mutable.Queue[ActorRef]()
  var outboundMeasurementRequests: mutable.Queue[Member] = mutable.Queue[Member]() // outbound: requests from this node to other nodes
  var outboundOngoingMeasurements: mutable.Queue[Member] = mutable.Queue[Member]()
  var pendingSlotRequest = false
  implicit val timeout = Timeout(new FiniteDuration(120, TimeUnit.SECONDS))

  override def receive: Receive = {

    case BandwidthMeasurementSlotRequest() => // sender requests a measurement slot so that only the sender and no other node starts iperf to this node (at the same time)
      val s = sender()
      val senderMember = TCEPUtils.getMemberFromActorRef(cluster, s)
      if(selfMeasurements.exists(dm => dm.node.address.equals(senderMember.address))) {
        s ! BandwidthMeasurementExists(this.getDataRate(senderMember).doubleValue())
      } else {
        inboundMeasurementRequests.enqueue(s)
        SpecialStats.debug(s"$this", s"bandwidth measurement slot reserved for $s; inbound queue: $inboundMeasurementRequests; inbound ongoing: $inboundOngoingMeasurements")
        //context.system.scheduler.scheduleOnce(new FiniteDuration(0, TimeUnit.SECONDS))(processInboundRequests)
      }
    case c: BandwidthMeasurementComplete =>
      val s = sender()
      inboundOngoingMeasurements.dequeue()
      this.addMeasurement(DataRateMeasurement(TCEPUtils.getMemberFromActorRef(cluster, s), c.bandwidth)) // store link measurement if no own value exists yet -> prevent duplicate link measurements
      SpecialStats.debug(s"$this", s"completed bandwidth measurement request ${s.path.address.host} --> ${cluster.selfAddress.host}, result: ${c.bandwidth}")
      TCEPUtils.selectSimulator(cluster).resolveOne().map(_.forward(c)) // forward completion message to simulator so it can record completion status

    case InitialBandwidthMeasurementStart() =>

      if(!initialized) {
        initialized = true
        if(isMininetSim) {
          log.info(s"running a mininet simulation, skipping bandwidth measurements and using value set in mininet setup: $mininetBandwidth")
          cluster.state.members.foreach(m => selfMeasurements += DataRateMeasurement(m, mininetBandwidth))
          cluster.state.members.map(m => cluster.state.members.map(other => allMeasurements += (m, other) -> mininetBandwidth))
        } else {
          SpecialStats.debug(s"$this", s"initial bandwidth measurements starting on actor $self")
          val membersSorted = cluster.state.members.filter(!_.hasRole("VivaldiRef")).toVector.sortBy(m => m.address.port.getOrElse(
            throw new RuntimeException(s"member ($m has no port!")))
          val selfPosition = membersSorted.indexOf(cluster.selfMember)
          val split: (Vector[Member], Vector[Member]) = membersSorted.splitAt(selfPosition)
          val memberProbeOrder: Vector[Member] = split._2 ++ split._1 // nodes are sorted by port; shift order so that not all nodes send their first request to the same node
          memberProbeOrder.foreach(m => if (!m.equals(cluster.selfMember)) outboundMeasurementRequests.enqueue(m)) // queue all
          // wait a bit so that not all nodes enqueue their first outbound request at once, which would cause a deadlock -> dining philosophers
          val firstRequestDelay = scala.util.Random.nextInt(10)
          context.system.scheduler.scheduleOnce(new FiniteDuration(firstRequestDelay, TimeUnit.SECONDS))(processOutboundRequests)
          context.system.scheduler.scheduleOnce(new FiniteDuration(0, TimeUnit.SECONDS))(processInboundRequests)
        }
      }

    case SingleBandwidthRequest(node: Member) =>
      val s = sender()
      val bandwidth = this.getDataRate(node).doubleValue()
      s ! SingleBandwidthResponse(bandwidth)

    case AllBandwidthsRequest() =>
      SpecialStats.log(this.toString, "placement", s"received AllBandwidthsRequest, replying with ${allMeasurements.size + selfMeasurements.size} entries")
      sender() ! AllBandwidthsResponse(allMeasurements.toMap ++ selfMeasurements.map(m => (cluster.selfMember, m.node) -> m.dataRate))

    case _ =>
  }

  // TODO test impact of allowing / disallowing simultaneous receiving and sending of measurements
  /**
    * handle requests from other nodes that want to contact the local iperf server
    * allow only one node to measure (send to this node) at a time to prevent wrong values
    * currently inbound (iperf server being used) and outbound (contacting remote iperf server) can NOT happen at the same time
    */
  def processInboundRequests: Unit = {
    // no other node is measuring bandwidth to this node, notify next node that it can start
    if (inboundOngoingMeasurements.isEmpty && outboundOngoingMeasurements.isEmpty && inboundMeasurementRequests.nonEmpty) {
      val requester = inboundMeasurementRequests.dequeue()
      requester ! BandwidthMeasurementSlotGranted()
      inboundOngoingMeasurements.enqueue(requester)
      if (inboundOngoingMeasurements.size > 1) throw new IllegalStateException(s"more than one bandwidth measurement at a time to this node ${cluster.selfMember} is being executed: ${inboundOngoingMeasurements}")
      SpecialStats.debug(s"$this", s"bandwidth measurement inbound queue: $inboundMeasurementRequests inbound ongoing: $inboundOngoingMeasurements outbound ongoing: $outboundOngoingMeasurements")
    }

    context.system.scheduler.scheduleOnce(new FiniteDuration(1, TimeUnit.SECONDS))(processInboundRequests)
  }

  /**
    * handle measurements from this node to other nodes' iperf servers
    * allow only one measurement (send to a remote node) at a time to prevent wrong values
    * currently inbound (iperf server being used) and outbound (contacting remote iperf server) can NOT happen at the same time
    */
  def processOutboundRequests: Unit = {

    if(outboundOngoingMeasurements.isEmpty && inboundOngoingMeasurements.isEmpty && outboundMeasurementRequests.nonEmpty && !pendingSlotRequest) {

      val otherNode = outboundMeasurementRequests.dequeue()
      pendingSlotRequest = true // signify that an outbound request is sent, but not yet granted
      val outboundProcessing = for {
          taskManager: ActorRef <- TCEPUtils.selectTaskManagerOn(cluster, otherNode.address).resolveOne()
          slotGranted <- taskManager ? BandwidthMeasurementSlotRequest() // wait for a BandwidthMeasurementSlotGranted() or BandwidthMeasurementExists() message
        } yield {
        outboundOngoingMeasurements.enqueue(otherNode)
        pendingSlotRequest = false
        SpecialStats.debug(s"$this", s"received reply to slot request to $otherNode: $slotGranted")
        slotGranted match {
          case BandwidthMeasurementSlotGranted() =>
            val measurement = this.measure(otherNode)
            // notify other node that measurement is complete -> free slot on other and send result for storage to prevent duplicate measurements
            measurement.onComplete {
              case Success(value) =>
                taskManager ! BandwidthMeasurementComplete(cluster.selfAddress, otherNode.address, value)
                SpecialStats.debug(s"$this", s"completed new bandwidth measurement ${cluster.selfAddress.host} --> ${otherNode.address.host}, result: $value")
                this.addMeasurement(DataRateMeasurement(otherNode, value))
                outboundOngoingMeasurements.dequeue()
              case Failure(exception) =>
                SpecialStats.debug(s"$this", s"failed new bandwidth measurement ${cluster.selfAddress.host} --> ${otherNode.address.host}, cause: \n $exception")
                outboundOngoingMeasurements.dequeue()
            }
          case BandwidthMeasurementExists(bandwidthFromOther) =>
            SpecialStats.debug(s"$this", s"bandwidth has been measured already ${cluster.selfAddress.host} --> ${otherNode.address.host}, result: $bandwidthFromOther")
            TCEPUtils.selectSimulator(cluster).resolveOne().map(_ ! BandwidthMeasurementComplete(cluster.selfAddress, otherNode.address, bandwidthFromOther)) // send completion message to simulator so it can record completion status
            this.addMeasurement(DataRateMeasurement(otherNode, bandwidthFromOther))
            outboundOngoingMeasurements.dequeue()
          case other =>
            log.error(s"received unknown message $other in reply to BandwidthMeasurementSlotRequest() to $taskManager")
            outboundOngoingMeasurements.dequeue()
            throw new RuntimeException(s"received unknown message $other in reply to BandwidthMeasurementSlotRequest() to $taskManager")
        }

        SpecialStats.debug(s"$this", s"outbound queue: $outboundMeasurementRequests; outbound ongoing: $outboundOngoingMeasurements inbound ongoing: $inboundOngoingMeasurements")
      }

      for { exc <- outboundProcessing.failed } yield { // recover from failures
        pendingSlotRequest = false
        outboundOngoingMeasurements.dequeue()
        outboundMeasurementRequests.enqueue(otherNode)
        log.error(s"failed outbound bandwidth measurement to $otherNode, requeueing it \n $exc")
      }
    }
    context.system.scheduler.scheduleOnce(new FiniteDuration(1, TimeUnit.SECONDS))(processOutboundRequests)
  }

  def addMeasurement(measurement: DataRateMeasurement): Unit = {
    val measurementEntry = selfMeasurements.find(_.node == measurement.node)
    if(measurementEntry.isEmpty)
      selfMeasurements += measurement
    else if(measurementEntry.get.dataRate != defaultRate) selfMeasurements += measurement
  }

  def addMeasurementNode(addNode: Member): Boolean = {
    val dataRateMeasurement = selfMeasurements.find(measurement => {
      measurement.node == addNode
    })
    if (dataRateMeasurement.isDefined) {
      log.info(s"Node already in measurement list $addNode")
      return false
    }
    selfMeasurements.append(DataRateMeasurement(addNode))
    log.info(s"Added data rate measurement node $addNode")
    true
  }

  def removeMeasurementNode(removeNode: Member): Unit = {
    // First find the DataRateMeasurement for this actor
    val dataRateMeasurement = selfMeasurements.find(measurement => {
      measurement.node == removeNode
    })
    if (dataRateMeasurement.isEmpty) {
      return
    }
    selfMeasurements -= dataRateMeasurement.get
    log.info(s"Removed node from measurement list $removeNode")
  }

  def getDataRate(node: Member): Number = {
    val dataRateMeasurement = selfMeasurements.find(measurement => {
      measurement.node == node
    })
    if (dataRateMeasurement.isEmpty) {
      log.info(s"Data rate measurement for $node not found, returning default $defaultRate")
      return defaultRate
    }
    else dataRateMeasurement.get.dataRate
  }

  private def updateMeasurements: Unit = {
    selfMeasurements.foreach(measurementNode => {
      log.debug(s"Updating data rate measurement for node ${measurementNode.node} with old measurement ${measurementNode.dataRate}")
      val measurement = measure(measurementNode.node)
      measurement.foreach { result => measurementNode.dataRate = result }
    })
  }

  private def measure(node: Member): Future[Double] = {

    val measurement = Future {
      if (node.equals(cluster.selfMember))
        0.0d
      else {
        // Check if there was a measurement in the last x seconds and return value immediately
        try {
          val pattern = "[\\d]*\\sKbits\\/sec".r
          val res = s"iperf3 -c ${node.address.host.getOrElse("localhost")} -t $measurementDuration -f k ".!!
          val rate = pattern.findAllIn(res).toList.last.split(" ")(0) // use average of 5 seconds
          rate.toDouble / 1000d // MBit/s
        } catch {
          case e: Throwable => {
            log.warning(s"Iperf failed for $node, using default value. \n ${e.toString}")
            defaultRate
          }
        }
      }
    }
    measurement.onComplete {
      case Success(result) => log.info(s"New data rate for node $node}: $result")
      case Failure(exception) => log.warning(s"failed to measure bandwidth to $node, cause: \n $exception")
    }
    measurement
  }

}

case class DataRateMeasurement(node: Member, var dataRate: Double = ConfigFactory.load().getDouble("constants.default-data-rate"))