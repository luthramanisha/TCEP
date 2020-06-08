package org.discovery.vivaldi

import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorRef, ActorSelection, ActorSystem, Address, Props}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberUp}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import tcep.ClusterActor
import tcep.machinenodes.helper.actors._
import tcep.utils.{SpecialStats, TCEPUtils}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


/**
  * Synchronizes with the other cluster nodes to calculate the coordinates
  * Assumes that all of the cluster nodes have `DistVivaldiActor`
  *
  * Created by raheel on 31/01/2018.
  */
class DistVivaldiActor extends ClusterActor with ActorLogging {

  private val errorThreshold = ConfigFactory.load().getDouble("constants.coordinates-error-threshold")
  //private val pool = Executors.newCachedThreadPool()
  //protected implicit val ec = ExecutionContext.fromExecutorService(pool)
  implicit val timeout = Timeout(ConfigFactory.load().getInt("constants.coordinate-request-timeout"), TimeUnit.SECONDS)
  private val refreshInterval = ConfigFactory.load().getInt("constants.coordinates-refresh-interval")
  private var updatesStarted = false
  private var coordsInitialized = false

  val refreshTask: Runnable = () => {

    val activeMembers = cluster.state.members.filter(m => m.status == MemberStatus.up && !m.equals(cluster.selfMember) && !m.address.equals(cluster.selfAddress))
    activeMembers.foreach(m => TCEPUtils.selectDistVivaldiOn(cluster, m.address) ! VivaldiPing(System.currentTimeMillis()))

    if (DistVivaldiActor.localPos.localConfidence >= errorThreshold)
      context.system.scheduler.scheduleOnce(refreshInterval / 5.0 seconds, refreshTask)
    else context.system.scheduler.scheduleOnce(refreshInterval seconds, refreshTask)
  }

  override def preStart(): Unit = {
    super.preStart()
    log.info("booting up DistVivaldiActor")
  }

  override def receive: Receive = {

    case StartVivaldiUpdates() =>
      if(!updatesStarted) {
        log.info("starting periodic coordinate updates")
        context.system.scheduler.scheduleOnce(0 seconds, refreshTask)
        updatesStarted = true
      }

    case CoordinatesRequest(address) =>
      val s = sender()
      DistVivaldiActor.getCoordinates(cluster, address) pipeTo s

    case ping: VivaldiPing => sender() ! VivaldiPong(ping.sendTime, DistVivaldiActor.localPos)

    case pong: VivaldiPong => {

      val s = sender()
      val rtt = System.currentTimeMillis() - pong.sendTime
      val latency: Double = rtt / 2.0d // time to other + time back / 2

      if (s.path.address != cluster.selfAddress) { // do NOT use pings from actors on the same host since vivaldi does not work well when estimating very small latencies
        try {
          DistVivaldiActor.coordinatesMap = DistVivaldiActor.coordinatesMap.updated(s.path.address, pong.receiverPosition.coordinates) // cache coords of sender
          val updateResult = DistVivaldiActor.localPos.update(latency, pong.receiverPosition.coordinates, pong.receiverPosition.localConfidence)
          if (updateResult) {
            DistVivaldiActor.updates += 1
            if(DistVivaldiActor.localPos.localConfidence <= errorThreshold && !coordsInitialized) {
              TCEPUtils.selectSimulator(cluster) ! VivaldiCoordinatesEstablished()
              coordsInitialized = true
            }
            val vivaldiDistance = DistVivaldiActor.localPos.coordinates.distance(pong.receiverPosition.coordinates)
            SpecialStats.log(s"${this.self}", "DistVivaldi", s"updated coordinates to ${DistVivaldiActor.localPos.coordinates}" +
              s"; ${s.path.address} : pos: ${pong.receiverPosition.coordinates} latency ${latency}ms | vivaldi distance: ${vivaldiDistance} -> absolute error ${math.abs(vivaldiDistance - latency)} relative error ${(vivaldiDistance - latency) / latency}")
          }
        } catch {
          case e: Throwable =>
            DistVivaldiActor.failedUpdates += 1
            log.info(s"failed to update coordinates with $pong from ${s}, \n cause: ${e.getMessage} ")
            //e.printStackTrace()
        }
      } else log.info(s"ignoring vivaldi pong from local actor, rtt was $rtt")
    }

    case MemberUp(member) => memberUp(member)
    case state: CurrentClusterState => currentClusterState(state)
    case _: MemberEvent => // ignore
  }

  def currentClusterState(state: CurrentClusterState): Unit = {
    val vivMembers = state.members.filter(m => m.status == MemberStatus.Up)
    for (member <- vivMembers) {
      val vivaldiRefActor = TCEPUtils.selectDistVivaldiOn(cluster, member.address)
      save(member, vivaldiRefActor)
    }
  }

  def memberUp(member: Member): Unit = {
    //Thread.sleep(5000)
    val vivaldiRefActor = TCEPUtils.selectDistVivaldiOn(cluster, member.address)
    save(member, vivaldiRefActor)
  }

  def save(member: Member, selection: ActorSelection) = {
    DistVivaldiActor.upMembers = DistVivaldiActor.upMembers.updated(member, selection)
  }

}

object DistVivaldiActor {

  private var distVivRef: ActorRef = _
  lazy val logger = LoggerFactory.getLogger(getClass)
  implicit val ec = ExecutionContext.Implicits.global // TODO which to use?
  implicit val timeout = Timeout(ConfigFactory.load().getInt("constants.coordinate-request-timeout"), TimeUnit.SECONDS)

  @volatile var localPos = VivaldiPosition.create()
  //val upMembers: ListBuffer[ActorSelection] = ListBuffer.empty
  var upMembers: Map[Member, ActorSelection] = Map()
  var initialized = false
  var updates: Int = 0
  var failedUpdates: Int = 0
  var coordinatesMap: Map[Address, Coordinates] = Map()

  def createVivIfNotExists(actorSystem: ActorSystem): ActorRef = if(!initialized) {
    initialized = true
    distVivRef = actorSystem.actorOf(Props(new DistVivaldiActor).withMailbox("prio-mailbox"), "DistVivaldiRef")
    logger.info(s"generated distributed vivaldi actor $distVivRef")
    SpecialStats.debug(s"$this", s"generated distributed vivaldi actor $distVivRef")
    distVivRef
  } else distVivRef

  def getLatencyValues(): List[LatencyDistance] = {
    var distances: ListBuffer[LatencyDistance] = new ListBuffer()
    for ((k1, v1) <- coordinatesMap) {
      for ((k2, v2) <- coordinatesMap) {
        distances += LatencyDistance(k1, v1, k2, v2, v1.distance(v2))
      }
      // Also calculate distance from cluster0 to all others because it is not included in the coordinatesMap
      val consumerAddress = Address("http", "tcep", "node0", 3000)
      distances += LatencyDistance(consumerAddress, this.localPos.coordinates, k1, v1, v1.distance(this.localPos.coordinates))
    }
    distances.toList
  }

  def getCoordinatesLocally(cluster: Cluster, address: Address): Option[Coordinates] = {

    if (address.equals(cluster.selfAddress) || address.host.isEmpty) Some(this.localPos.coordinates)
    else if (this.coordinatesMap.contains(address)) Some(this.coordinatesMap(address))
    else None
  }

    def getCoordinates(cluster: Cluster, address: Address): Future[CoordinatesResponse] = {

      val startTime = System.currentTimeMillis()
      val localRequest = this.getCoordinatesLocally(cluster, address)
      if(localRequest.isDefined) Future { CoordinatesResponse(localRequest.get) }
      else { // no coordinates of remote node available
        SpecialStats.debug(s"$this", s"received coord request for $address, no entry exists -> asking explicitly")
        logger.warn(s"coordinate request for  $address failed since no local entry is available yet, asking explicitly")
        val remoteRequest = for {
          distViv <- TCEPUtils.selectDistVivaldiOn(cluster, address).resolveOne()
          response <- (distViv ? CoordinatesRequest(address)).mapTo[CoordinatesResponse]
        } yield {
          this.coordinatesMap += address -> response.coordinate
          response
        }

        remoteRequest.onComplete {
          case scala.util.Success(coords) =>
            SpecialStats.debug(s"$this", s"retrieving coordinates from $address  took ${System.currentTimeMillis() - startTime}ms")
          case scala.util.Failure(exception) =>
            SpecialStats.debug(s"$this", s"failed to retrieve coordinates from $address after ${System.currentTimeMillis() - startTime}ms \n $exception")
        }
        remoteRequest
      }
    }
}

case class LatencyDistance(member1: Address, coord1: Coordinates, member2: Address, coord2: Coordinates, distance: Double)

