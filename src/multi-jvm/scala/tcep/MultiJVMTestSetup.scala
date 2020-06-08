package tcep

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSelection, Address, Props}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.{Coordinates, DistVivaldiActor}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.slf4j.{Logger, LoggerFactory}
import tcep.data.Events.Event1
import tcep.data.Queries.{Filter1, Query, Stream1}
import tcep.machinenodes.helper.actors.{InitialBandwidthMeasurementStart, StartVivaldiUpdates, TaskManagerActor}
import tcep.publishers.RegularPublisher
import tcep.utils.TCEPUtils

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * basic setup class for tests that need a Cluster
  * run using sbt multi-jvm:test or sbt multi-jvm:testOnly *classprefix*
  */
abstract class MultiJVMTestSetup(numNodes: Int = 5) extends MultiNodeSpec(config = TCEPMultiNodeConfig)
  with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {

  import TCEPMultiNodeConfig._
  val logger: Logger = LoggerFactory.getLogger(getClass)

  var cluster: Cluster = _
  var node1Ref, node2Ref, clientRef, clientTaskRef, publisher1Ref: ActorRef = _
  val pNames = Vector("P:localhost:2501", "P:localhost:2502")
  implicit val creatorAddress: Address = cluster.selfAddress
  implicit var ec: ExecutionContext = _
  val errorMargin = 0.05

  override def initialParticipants = numNodes

  override def beforeAll() = {
    multiNodeSpecBeforeAll()
    cluster = Cluster(system)
    ec = ExecutionContext.Implicits.global

    runOn(node1) {
      node1Ref = system.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
      DistVivaldiActor.createVivIfNotExists(system)
    }
    runOn(node2) {
      node2Ref = system.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
      DistVivaldiActor.createVivIfNotExists(system)
    }
    runOn(client) {
      clientTaskRef = system.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
      DistVivaldiActor.createVivIfNotExists(system)
    }
    runOn(publisher1) {
      // taskmanager for bandwidth measurements, not for hosting operators
      system.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
      publisher1Ref = system.actorOf(Props(RegularPublisher(500, id => Event1(id))), pNames(0))
      DistVivaldiActor.createVivIfNotExists(system)
    }
    runOn(publisher2) {
      // taskmanager for bandwidth measurements, not for hosting operators
      system.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
      publisher1Ref = system.actorOf(Props(RegularPublisher(500, id => Event1(id))), pNames(1))
      DistVivaldiActor.createVivIfNotExists(system)
    }
    cluster join node(client).address

    var upMembers = 0
    do {
      upMembers = cluster.state.members.count(_.status == MemberStatus.up)
    } while(upMembers < initialParticipants)
    testConductor.enter("wait for startup")

  }

  override def afterAll() = multiNodeSpecAfterAll()

  def startVivaldiCoordinateUpdates: Unit = cluster.state.members.foreach(
    m => TCEPUtils.selectDistVivaldiOn(cluster, m.address) ! StartVivaldiUpdates())

  def setCoordinatesExplicitly(clientCoords: Coordinates, publisher1Coords: Coordinates, publisher2Coords: Coordinates,
                                host1: Coordinates, host2: Coordinates): Unit = {

    runOn(client) {
      DistVivaldiActor.localPos.coordinates = clientCoords
    }
    runOn(publisher1) {
      DistVivaldiActor.localPos.coordinates = publisher1Coords
    }
    runOn(publisher2) {
      DistVivaldiActor.localPos.coordinates = publisher2Coords
    }
    runOn(node1) {
      DistVivaldiActor.localPos.coordinates = host1
    }
    runOn(node2) {
      DistVivaldiActor.localPos.coordinates = host2
    }
    DistVivaldiActor.coordinatesMap = Map()
  }

  def startBandwidthMeasurements: Unit = cluster.state.members.foreach(
    m => TCEPUtils.selectTaskManagerOn(cluster, m.address) ! InitialBandwidthMeasurementStart())


  def getMembersWithRole(cluster: Cluster, role: String) = cluster.state.members.filter(m => m.hasRole(role))

  def printActors(): Unit = println(s"ACTORREFs \n $node1Ref  \n $node2Ref \n $clientTaskRef \n $clientRef \n $publisher1Ref")


  def getPublisherOn(cluster: Cluster, node: Member, name: String): Option[ActorRef] = {
    val actorSelection: ActorSelection = cluster.system.actorSelection(node.address.toString + s"/user/$name")
    val actorResolution: Future[ActorRef] = actorSelection.resolveOne()(Timeout(15, TimeUnit.SECONDS))
    try {
      // using blocking variant here, otherwise return type would have to be future
      val result = Await.result(actorResolution, new FiniteDuration(15, TimeUnit.SECONDS))
      Some(result)
    } catch {
      case e: Throwable =>
        log.error(s"publisher on ${node} not found \n $e")
        None
    }
  }
  def getTestBDP(mapping: Map[Query, (Member, Coordinates)], client: Coordinates, publisher: Coordinates): Double = {
    val stream = mapping.find(e => e._1.isInstanceOf[Stream1[Int]]).get._1
    val filter = mapping.find(e => e._1.isInstanceOf[Filter1[Int]]).get._1
    val latency1 = publisher.distance(mapping(stream)._2)
    val latency2 = mapping(stream)._2.distance(mapping(filter)._2)
    val latency3 = client.distance(mapping(filter)._2)
    val bw = ConfigFactory.load().getDouble("constants.default-data-rate")
    latency1 * bw + latency2 * bw + latency3 * bw
  }

}
