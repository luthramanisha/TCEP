package tcep

import java.util.concurrent.TimeUnit

import akka.actor.ActorContext
import akka.cluster.{Cluster, Member}
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.Coordinates
import org.scalatest.mockito.MockitoSugar
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.placement.{GlobalOptimalBDPAlgorithm, HostInfo, PlacementStrategy}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

// need one concrete test class per node
class GlobalOptimalBDPMultiJvmNode1 extends      GlobalOptimalBDPMultiNodeTestSpec
class GlobalOptimalBDPMultiJvmNode2 extends      GlobalOptimalBDPMultiNodeTestSpec
class GlobalOptimalBDPMultiJvmClient extends     GlobalOptimalBDPMultiNodeTestSpec
class GlobalOptimalBDPMultiJvmPublisher1 extends GlobalOptimalBDPMultiNodeTestSpec
class GlobalOptimalBDPMultiJvmPublisher2 extends GlobalOptimalBDPMultiNodeTestSpec

abstract class GlobalOptimalBDPMultiNodeTestSpec extends MultiJVMTestSetup with MockitoSugar {

  import TCEPMultiNodeConfig._

  private def initUUT(cluster: Cluster): GlobalOptimalBDPAlgorithm.type = {
    val uut = GlobalOptimalBDPAlgorithm
    val publisher1 = getPublisherOn(cluster, cluster.state.members.find(_.address.port.get == 2501).get, pNames(0)).get
    val publisher2 = getPublisherOn(cluster, cluster.state.members.find(_.address.port.get == 2502).get, pNames(1)).get
    val publishers = Map(pNames(0) -> publisher1, pNames(1) -> publisher2) // need to override publisher map here because all nodes have hostname localhost during test

    Await.result(uut.initialize(cluster, publisherNameMap = Some(publishers)), FiniteDuration(20, TimeUnit.SECONDS))
    assert(uut.cluster != null)
    testConductor.enter("initialization complete")
    uut
  }

  def setCoordinates(uut: PlacementStrategy,
                     clientCoords: Coordinates, publisher1Coords: Coordinates, publisher2Coords: Coordinates,
                     host1: Coordinates, host2: Coordinates): Unit = {

    this.setCoordinatesExplicitly(clientCoords, publisher1Coords, publisher2Coords, host1, host2)
    uut.memberCoordinates = Map()
    val coordUpdate = uut.updateCoordinateMap()
    Await.result(coordUpdate, new FiniteDuration(30, TimeUnit.SECONDS))
  }

  "GlobalOptimalBDPAlgorithm" must {
    "select the node with minimum BDP sum to publishers and subscriber" in {

      val uut = initUUT(cluster)
      val clientC = new Coordinates(0, -10, 0)
      val publisher1 = new Coordinates(-40, 30, 0) // pnames(0)
      val publisher2 = new Coordinates(40, 30, 0) // pnames(1)
      val host1 = new Coordinates(0, 0, 0)
      val host2 = new Coordinates(40, -10, 0)
      setCoordinates(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {

        val s1 = Stream1[Int](pNames(0), Set())
        val s2 = Stream1[Int](pNames(1), Set())
        val and = Conjunction11[Int, Int](s1, s2, Set())
        val f = Filter2[Int, Int](and, _ => true, Set())
        val operators: List[Query] = List(s1, s2, and, f)
        //val publishers = Map(pNames(0) -> getPublisherOn(cluster, getMembersWithRole(cluster, "Publisher").head, pNames(0)).get)
        val candidates = Await.result(uut.getCoordinatesOfMembers(cluster, cluster.state.members), uut.requestTimeout)

        def getBDPOfPlacement(rootOperator: Query, placement: Map[Query, Member], coords: Map[Member, Coordinates]): Double = {
          val bw = ConfigFactory.load().getDouble("constants.default-data-rate")
          val opToParentBDPs = placement.map(op => op._1 -> {
            val opCoords = coords(op._2)
            val bdpToParents = op._1 match { // parent == subquery
                // find stream publisher by member address port (see pNames)
              case s: StreamQuery => bw * opCoords.distance(coords.find(e => s.publisherName.contains(e._1.address.port.get.toString)).get._2)
              case s: SequenceQuery =>
                bw * opCoords.distance(coords.find(e => s.s1.publisherName.contains(e._1.address.port.get.toString)).get._2) +
                  bw * opCoords.distance(coords.find(e => s.s2.publisherName.contains(e._1.address.port.get.toString)).get._2)
              case b: BinaryQuery => bw * opCoords.distance(coords(placement(b.sq1))) + bw * opCoords.distance(coords(placement(b.sq2)))
              case u: UnaryQuery => bw * opCoords.distance(coords(placement(u.sq)))
            }
            if(op._1 == rootOperator) bdpToParents + bw * opCoords.distance(coords.find(_._1.hasRole("Subscriber")).get._2)
            else bdpToParents
          })
          opToParentBDPs.values.sum
        }
        // all placements where all operators are on one host
        val allPossibleMappings: List[Map[Query, Member]] = candidates.map(c => operators.map(op => op -> c._1).toMap).toList
        val BDPs = allPossibleMappings.map(m => m -> getBDPOfPlacement(f, m, candidates))
        val minBDP = BDPs.minBy(m => m._2)._2
        //println(s"\nDEBUG BDPs: ${BDPs.map(e => s"\n ${e._1.map(t => s"${t._1} -> ${t._2.address.port.get}")}  | ${e._2}")}")
        //println(s"\nDEBUG minimum BDP: $minBDP")
        val dependencies = Dependencies(List(), List()) // only relevant for bdp calculation, not tested here
        val placement: Map[Query, Member] = operators.map(op => op -> Await.result(uut.findOptimalNode(mock[ActorContext], cluster, dependencies, mock[HostInfo], op), uut.requestTimeout).member).toMap
        val placementBDP = getBDPOfPlacement(f, placement, candidates)
        //println(s"\n placement bdp: $placementBDP \n ${placement.mkString("\n")}")
        val operatorHosts = placement.values
        operatorHosts.foreach(m => assert(!operatorHosts.exists(_ != m), "all operators must be on the same host"))
        assert(placementBDP == minBDP, "BDP of placement must be minimum possible BDP of all possible placements with all operators on one node")
      }

      testConductor.enter("test minimal BDP complete")
    }
  }

}