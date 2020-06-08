package tcep.machinenodes.helper.actors

import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorRef, Address, Props}
import akka.cluster.Member
import akka.pattern.pipe
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.{Coordinates, VivaldiPosition}
import tcep.data.Queries.Query
import tcep.factories.NodeFactory
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.placement.manets.StarksAlgorithm
import tcep.placement.vivaldi.VivaldiCoordinates
import tcep.placement.{BandwidthEstimator, HostInfo}
import tcep.simulation.adaptive.cep.SystemLoad
import tcep.utils.{SpecialStats, TCEPUtils}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by raheel
  * on 09/08/2017.
  */
sealed trait Message extends Serializable // priority is configured in TCEPPrioMailbox
trait TransitionControlMessage extends Message //Only for transition related control messages, highest priority in TCEPPriorityMailbox
trait PlacementMessage extends TransitionControlMessage
trait MeasurementMessage extends Message // messages for measurements needed for placements
trait VivaldiCoordinatesMessage extends Message // regular vivaldi coordinate updates

case class CreateRemoteOperator(operatorInfo: HostInfo, props: Props) extends TransitionControlMessage
case class RemoteOperatorCreated(ref: ActorRef) extends TransitionControlMessage
case class StarksTask(operator: Query, producers: Seq[ActorRef], askerInfo: HostInfo) extends PlacementMessage {
  override def toString: String = s"operator: ${askerInfo.operator.toString.split("\\(").head}; source: ${askerInfo.member.address}; visited: ${askerInfo.visitedMembers.map(_.address)}"

}
case class StarksTaskReply(hostInfo: HostInfo) extends PlacementMessage

case class LoadRequest() extends MeasurementMessage
case class LoadResponse(load: Double) extends MeasurementMessage
case class SingleBandwidthRequest(target: Member) extends MeasurementMessage
case class SingleBandwidthResponse(bandwidth: Double) extends MeasurementMessage
case class AllBandwidthsRequest() extends MeasurementMessage
case class AllBandwidthsResponse(bandwidthMap: Map[(Member, Member), Double]) extends MeasurementMessage
case class BDPRequest(target: Member) extends MeasurementMessage
case class BDPResponse(bdp: Double) extends MeasurementMessage
case class InitialBandwidthMeasurementStart() extends PlacementMessage
case class BandwidthMeasurementSlotRequest() extends PlacementMessage
case class BandwidthMeasurementSlotGranted() extends PlacementMessage
case class BandwidthMeasurementExists(bandwidth: Double) extends PlacementMessage
case class BandwidthMeasurementComplete(source: Address, target: Address, bandwidth: Double) extends PlacementMessage
case class CoordinatesRequest(node: Address) extends MeasurementMessage // address field is only intended for asking local vivaldi actor about a remote addresses coordinates
case class CoordinatesResponse(coordinate: Coordinates) extends MeasurementMessage
case class GetNetworkHopsMap() extends MeasurementMessage
case class NetworkHopsMap(hopsMap: Map[String, Int]) extends MeasurementMessage

case class StartVivaldiUpdates() extends PlacementMessage
case class VivaldiCoordinatesEstablished() extends PlacementMessage
case class VivaldiPing(sendTime: Long) extends VivaldiCoordinatesMessage
case class VivaldiPong(sendTime: Long, receiverPosition: VivaldiPosition) extends VivaldiCoordinatesMessage

case class PublisherActorRefsRequest() extends PlacementMessage
case class PublisherActorRefsResponse(publisherMap: Map[String, ActorRef]) extends PlacementMessage
case class SetPublisherActorRefs(publisherMap: Map[String, ActorRef]) extends PlacementMessage
case class SetPublisherActorRefsACK() extends PlacementMessage
case class GetTaskManagerActor(node: Member) extends PlacementMessage
case class TaskManagerActorResponse(maybeRef: Option[ActorRef]) extends PlacementMessage
case class TaskManagerFound(member: Member, ref: ActorRef) extends PlacementMessage
case class AllTaskManagerActors(refs: Map[Member, ActorRef]) extends PlacementMessage
case class ACK() extends PlacementMessage // acknowledge msg for confirming transition-related message delivery (so we can retry if it's missing)

/**
  * responsible for handling placement-related activities
  */
class TaskManagerActor extends VivaldiCoordinates with ActorLogging {

  override implicit val ec = blockingIoDispatcher // almost all tasks are i/o bound
  private var bandwidthEstimator: ActorRef = _
  private val timeout = Timeout(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS)
  private val hostIpAddresses: List[String] = ConfigFactory.load().getStringList("constants.host-ip-addresses").asScala.toList
  private var publisherActorRefs: Map[String, ActorRef] = Map()
  private var taskManagerActors: Map[Member, ActorRef] = Map()
  private var publisherActorRefsInitialized = false
  @volatile private var ongoingPlacementRequests: Map[ActorRef, Future[HostInfo]] = Map()
  @volatile private var ongoingCreationRequests: Map[(ActorRef, Query), Option[ActorRef]] = Map() // requester ActorRef, created operator ActorRef -> keep track of who requested which operators to be created, re-send ActorRef if lost
  def clearPlacementRequest(actorRef: ActorRef): Unit = ongoingPlacementRequests = ongoingPlacementRequests.-(actorRef)
  def clearCreationRequest(key: (ActorRef, Query)) : Unit = ongoingCreationRequests = ongoingCreationRequests.-(key)

  override def preStart(): Unit = {
    super.preStart()
    bandwidthEstimator = cluster.system.actorOf(Props(new BandwidthEstimator()), "BandwidthEstimator")
    log.info(s"starting taskManager on ${cluster.selfMember} as ${this.self} with mailbox type ${this.context.props.mailbox} and bandwidthEstimator $bandwidthEstimator")
  }

  override def receive: Receive = super.receive orElse {

    case CreateRemoteOperator(operatorInfo, props) =>
      val s = sender()
      val key = (s, operatorInfo.operator)
      log.info(s"$self received CreateRemoteOperator $key, request already exists: ${ongoingCreationRequests.contains(key)}, operator created: ${ongoingCreationRequests.getOrElse(key, None)}")
      if(!ongoingCreationRequests.contains(key)) { // ignore re-sends of requests by GuaranteedMessageDeliverer (due to lost ACKs)
        ongoingCreationRequests += key -> None
        for {
          ref <- NodeFactory.createOperator(cluster, context, operatorInfo, props)(blockingIoDispatcher)
        } yield {
          ongoingCreationRequests += key -> Some(ref)
          log.info(s"operator creation request for $key complete, $ref sent back to $s")
          s ! RemoteOperatorCreated(ref)
          context.system.scheduler.scheduleOnce(timeout.duration)(clearCreationRequest(key)) // remove operator creation request after delivery timeout
        }
      } else if(ongoingCreationRequests(key).isDefined) {
        s ! RemoteOperatorCreated(ongoingCreationRequests(key).get) // received request before and actor is created -> re-send actor ref
      }
      // request received, actor creation not yet complete -> wait for next request

    case st: StarksTask =>
      implicit val ec: ExecutionContext = blockingIoDispatcher
      val s = sender()
      SpecialStats.debug(s"$this ", s"$s sent starks task $st, is first request: ${!ongoingPlacementRequests.contains(s)}")
      if(!ongoingPlacementRequests.contains(s)) {
        val starks = StarksAlgorithm
        val hostRequest: Future[HostInfo] = starks.findOptimalNode(this.context, cluster, Dependencies(st.producers.toList, List()), st.askerInfo, st.operator)
        ongoingPlacementRequests += s -> hostRequest
        hostRequest.onComplete(_ => context.system.scheduler.scheduleOnce(timeout.duration)(clearPlacementRequest(s))) // delay clearing a bit to avoid re-send arriving while sending reply
        hostRequest.map(StarksTaskReply(_)) pipeTo s
      } else ongoingPlacementRequests(s).map(StarksTaskReply(_)) pipeTo s


    case LoadRequest() =>
      val s = sender()
       s ! SystemLoad.getSystemLoad

    case BDPRequest(target: Member) =>
      val s = sender()
      TCEPUtils.getBDPBetweenNodes(cluster, cluster.selfMember, target)(blockingIoDispatcher) pipeTo s // this is a future

    case request: SingleBandwidthRequest => bandwidthEstimator.forward(request)
    case r: AllBandwidthsRequest => bandwidthEstimator.forward(r)
    case i: InitialBandwidthMeasurementStart => bandwidthEstimator.forward(i)
    case r: BandwidthMeasurementSlotRequest => bandwidthEstimator.forward(r)
    case c: BandwidthMeasurementComplete => bandwidthEstimator.forward(c)
    case PublisherActorRefsRequest() => sender() ! PublisherActorRefsResponse(this.publisherActorRefs)
    case SetPublisherActorRefs(publisherMap) =>
      val s = sender()
      if(s.path.name.contains("SimulationSetup")) {
        if(!publisherActorRefsInitialized) {
          this.publisherActorRefs = publisherMap
          s ! SetPublisherActorRefsACK()
          log.info("received publisher actorRef map")
          publisherActorRefsInitialized = true
        }
      }
      else log.error(s"received SetPublisherActorRefs msg from actor that is not simulationSetup ($s)")

    case t: AllTaskManagerActors => taskManagerActors = t.refs

    case GetTaskManagerActor(node) => sender() ! TaskManagerActorResponse(taskManagerActors.get(node))
    case other => log.info(s"ignoring unknown message $other")
  }
}