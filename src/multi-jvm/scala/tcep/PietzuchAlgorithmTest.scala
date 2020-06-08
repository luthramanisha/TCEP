package tcep.placement.sbon

import java.util.concurrent.TimeUnit

import akka.cluster.{Cluster, Member}
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries._
import tcep.dsl.Dsl.{Seconds => _, TimespanHelper => _, _}
import tcep.placement.{PlacementStrategy, QueryDependencies}
import tcep.{MultiJVMTestSetup, TCEPMultiNodeConfig}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._


// need one concrete test class per node
class PietzuchMultiJvmNode1 extends PietzuchMultiNodeTestSpec
class PietzuchMultiJvmNode2 extends PietzuchMultiNodeTestSpec
class PietzuchMultiJvmClient extends PietzuchMultiNodeTestSpec
class PietzuchMultiJvmPublisher1 extends PietzuchMultiNodeTestSpec
class PietzuchMultiJvmPublisher2 extends PietzuchMultiNodeTestSpec

abstract class PietzuchMultiNodeTestSpec extends MultiJVMTestSetup {

  import TCEPMultiNodeConfig._

  private def initUUT(cluster: Cluster): PietzuchAlgorithm.type = {
    val uut = PietzuchAlgorithm
    println(s"cluster: ${cluster.state.members}")
    println(s"${cluster.state.members.find(_.hasRole("Publisher"))}")

    val publisher1 = getPublisherOn(cluster, cluster.state.members.filter(_.hasRole("Publisher")).head, pNames(0)).get
    val publisher2 = getPublisherOn(cluster, cluster.state.members.find(_.hasRole("Publisher")).last, pNames(1)).get
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
    Await.result(coordUpdate, new FiniteDuration(10, TimeUnit.SECONDS))
  }

  "PlacementStrategy getNClosestNeighbours" must {
    "return the two closest neighbours" in within(new FiniteDuration(60, TimeUnit.SECONDS)) {

      val uut = initUUT(cluster)
      val clientCoords = new Coordinates(20, 0, 0)
      val publisher1 = new Coordinates(30, 0, 0)
      val publisher2 = new Coordinates(3000, 0, 0)
      val host1 = new Coordinates(40, 0, 0)
      val host2 = new Coordinates(10, 0, 0)
      setCoordinates(uut, clientCoords, publisher1, publisher2, host1, host2)

      runOn(client) {
        val res = uut.getNClosestNeighboursToCoordinatesByMember(2, new Coordinates(50, 0, 0))
        res should have size 2
        res.head._2.x should be(40.0d)
        res.last._2.x should be(30.0d)
      }
      testConductor.enter("test1-complete")
    }
  }

  "PlacementStrategy - getCoordinatesOfNode" must {
    "return the coordinates of a different cluster member" in within(new FiniteDuration(30, TimeUnit.SECONDS)) {

      val uut = initUUT(cluster)
      val clientCoords = new Coordinates(20, 0, 0)
      val publisher1 = new Coordinates(30, 0, 0)
      val publisher2 = new Coordinates(3000, 0, 0)
      val host1 = new Coordinates(40, 0, 0)
      val host2 = new Coordinates(10, 0, 0)
      setCoordinates(uut, clientCoords, publisher1, publisher2, host1, host2)

      runOn(node1) {
        val members = cluster.state.members
        val cres: Coordinates = uut.getCoordinatesOfNodeBlocking(cluster, members.find(m => m.hasRole("Subscriber")).get)
        cres should not be Coordinates.origin
        assert(cres.equals(new Coordinates(20, 0, 0)))
        val p1 = uut.getCoordinatesOfNodeBlocking(cluster, members.find(m => m.address.port.get == 2501).get)
        val p2 = uut.getCoordinatesOfNodeBlocking(cluster, members.find(m => m.address.port.get == 2502).get)
        assert(p1.equals(new Coordinates(30, 0, 0)))
        assert(p2.equals(new Coordinates(3000, 0, 0)))
      }
      testConductor.enter("test2-complete")
    }
  }

  "PlacementStrategy - getLoadOfNode" must {
    "return the load percentage of the machine (between 0 and 8)" in within(new FiniteDuration(15, TimeUnit.SECONDS)) {

      val uut = initUUT(cluster)

      runOn(node1) {
        val members = getMembersWithRole(cluster, "Candidate")
        val res: Double = Await.result(uut.getLoadOfNode(cluster, members.find(m => !m.equals(cluster.selfMember)).get), uut.resolveTimeout.duration)
        assert(res <= 18.0d && res >= 0.0d)
      }
      testConductor.enter("test-complete")
    }
  }

  "PietzuchAlgorithm - calculateVCSingleOperator() with one parent" must {
    "return virtual coordinates that are closer to the client node than the single parent node" in {

      val uut = initUUT(cluster)
      val clientC = new Coordinates(50, 50, 0)
      val publisher1 = new Coordinates(0, 0, 0)
      val publisher2 = new Coordinates(0, 0, 0)
      val host1 = new Coordinates(50, 0, 0)
      val host2 = new Coordinates(125, 125, 0)
      setCoordinates(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val op = stream[Int](pNames(0))
        val nnCandidates = uut.memberCoordinates.filter(t => t._1.hasRole("Candidate")).toMap
        val virtualCoords = Await.result(uut.calculateVCSingleOperator(cluster, op, Coordinates(0,0,0), List(new Coordinates(50, 0, 0), new Coordinates(50, 50, 0)), nnCandidates), uut.requestTimeout)
        assert(virtualCoords.distance(new Coordinates(50, 25, 0)) <= 6.0d, "virtual coordinates should be in the middle of parent and client")
        //assert(virtualCoords.distance(client) > virtualCoords.distance(host1),"virtual coords should be closer to parent because of reduced client influence")
        //val defaultBandwidth = BandwidthEstimator.defaultRate / 1000d
        //val clientweight = 1.0d // from when client acted as stand-in for children
        //val parentweight = 1.0d
        //val effectiveWeight = (clientweight - parentweight) * defaultBandwidth
        //val middle = (client.y - parent.y) / 2
        //val expectedY = middle + (effectiveWeight * middle)
        //assert(virtualCoords.distance(new Coordinates(50, expectedY, 0)) <= 1.0d, "virtual coords should be closer to parents because client influence is 0.25 of parent influence")
        //assert(virtualCoords.distance(client) < parent.distance(client) &&
        //  virtualCoords.distance(parent) < parent.distance(client), "virtual coords must lie between parent and client")
      }
      testConductor.enter("test-calculateVCSingleOperator-1-complete")
    }
  }

  "PietzuchAlgorithm - calculateVCSingleOperator() with two parents" must {
    "return virtual coordinates that are closer to the client node than the parent nodes" in within(new FiniteDuration(15, TimeUnit.SECONDS)) {

      val uut = initUUT(cluster)
      val clientC = new Coordinates(50, 50, 0)
      val publisher1 = Coordinates.origin
      val publisher2 = Coordinates.origin
      val host1 = new Coordinates(0, 50, 0)
      val host2 = new Coordinates(50, 0, 0)
      setCoordinates(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val op = stream[Int](pNames(0))
        val nnCandidates = uut.memberCoordinates.filter(t => t._1.hasRole("Candidate")).toMap
        val virtualCoords = Await.result(uut.calculateVCSingleOperator(cluster, op, Coordinates(0,0,0), List(clientC, host1, host2), nnCandidates), uut.requestTimeout)
        assert(virtualCoords.x > 25 && virtualCoords.y > 25 &&
          virtualCoords.x < 50 && virtualCoords.y < 50,
          "virtual coords should lie somewhere on the line between (25,25,0) and (50,50,0")
        assert(
          virtualCoords.distance(clientC) < host1.distance(clientC) && virtualCoords.distance(host1) < host1.distance(clientC) &&
            virtualCoords.distance(clientC) < host2.distance(clientC) && virtualCoords.distance(host2) < host1.distance(clientC),
          "virtual coords must lie between parents and client")
      }
      testConductor.enter("test-calculateVCSingleOperator()-2-complete")
    }
  }

  "PietzuchAlgorithm - findHost" must {
    "return the candidate Member that has minimal load among the 3 nodes closest to the virtual coords" in {
      val uut = initUUT(cluster)
      val clientC = new Coordinates(50, 50, 0)
      val publisher1C = new Coordinates(25, 25, 0)
      val publisher2 = new Coordinates(25, 25, 0)
      val host1 = new Coordinates(50, 0, 0)
      val host2 = new Coordinates(0, 50, 0)
      setCoordinates(uut, clientC, publisher1C, publisher2, host1, host2)

      runOn(client) {
        val op = stream[Int](pNames(0))
        val candidates = uut.memberCoordinates.filter(!_._1.hasRole("Publisher")).toMap
        val vc = new Coordinates(50, 50, 0)
        val result = Await.result(uut.findHost(vc, candidates, op, List()), uut.requestTimeout)
        val candidateLoads = candidates.map(c => c._1 -> Await.result(uut.getLoadOfNode(cluster, c._1), uut.resolveTimeout.duration))
        assert(getMembersWithRole(cluster, "Candidate").contains(result.member) && !result.member.equals(getMembersWithRole(cluster, "Publisher").head),
          "must place operator among the three closest non-publisher nodes")
        assert(!candidateLoads.values.toSeq.sortWith(_ < _).take(uut.k).exists(l => l < candidateLoads(result.member)), "result must be the candidate with minimum load")
      }
    }
    testConductor.enter("test physical placement complete")
  }

  "PietzuchAlgorithm - extractOperators()" must {
    "return a map of all operators and their assorted parent and child operators that exist in a query" in {

      val uut = initUUT(cluster)
      runOn(client) {
        val query =
          stream[Boolean]("A")
            .and(stream[Boolean]("B"))
            .or(stream[Boolean]("C").where(_ == true))

        val result = uut.extractOperators(query = query, child = None, mutable.LinkedHashMap[Query, QueryDependencies]())

        assert(result.nonEmpty)
        assert(result.size == 6, "result must contain all 6 operators")
        assert(result.count(o => o._1.isInstanceOf[Stream1[Boolean]]) == 3, "result must contain 3 stream operators")
        assert(result.count(o => o._1.isInstanceOf[Filter1[Boolean]]) == 1, "result must contain one filter operator")
        assert(result.count(o => o._1.isInstanceOf[Disjunction21[Boolean, Boolean, Boolean]]) == 1,
          "result must contain one disjunction operator")
        assert(result.count(o => o._1.isInstanceOf[Conjunction11[Boolean, Boolean]]) == 1,
          "result must contain one conjunction operator")
        assert(!result.filter(o => o._1.isInstanceOf[Stream1[Boolean]]).exists(o => o._2.parents.isDefined),
          "stream operators must not have parents")
        assert(result.exists(o => o._1.isInstanceOf[Disjunction21[Boolean, Boolean, Boolean]] && o._2.child.isEmpty),
          "disjunction operator is the root of the query and has no children")
        assert(result.exists(o => o._1.isInstanceOf[Disjunction21[Boolean, Boolean, Boolean]] &&
          o._2.parents.get.exists(p => p.isInstanceOf[Filter1[Boolean]]) &&
          o._2.parents.get.exists(p => p.isInstanceOf[Conjunction11[Boolean, Boolean]])),
          "disjunction operator has filter and conjunction as parents")
      }
      testConductor.enter("test extractOperators complete")
    }
  }

  "PietzuchAlgorithm - calculateVirtualPlacementWithCoords() with one operator" must {
    "return the mapping of operator to coordinates for the given query" in {
      val uut = initUUT(cluster)
      val clientC = new Coordinates(0, 0, 0)
      val publisher1 = new Coordinates(-100, 0, 0)
      val publisher2 = new Coordinates(-100, 0, 0)
      val host1 = new Coordinates(-75, 0, 0)
      val host2 = new Coordinates(-50, 0, 0)
      setCoordinates(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val candidateCoordinates = Await.result(uut.getCoordinatesOfMembers(cluster, uut.findPossibleNodesToDeploy(cluster)), uut.requestTimeout)
        val query = stream[Int](pNames(0))

        val result = uut.calculateVirtualPlacementWithCoords(cluster, query, clientC, Map(pNames(0) -> publisher1), candidateCoordinates)
        assert(result(query).distance(new Coordinates(-50, 0, 0)) <= 2.0, "single operator should be placed between client and publisher")
      }
      testConductor.enter("test calculateVirtualPlacementWithCoords() with one operator complete")
    }
  }

  "PietzuchAlgorithm - calculateVirtualPlacementWithCoords()" must {
    "return the mapping of operator to coordinates for the given query" in {
      val uut = initUUT(cluster)
      val clientC = new Coordinates(0, 0, 0)
      val publisher1 = new Coordinates(-100, 0, 0)
      val publisher2 = new Coordinates(-100, 0, 0)
      val host1 = new Coordinates(-66.666, 0, 0)
      val host2 = new Coordinates(-33.333, 0, 0)
      setCoordinates(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val candidateCoordinates = Await.result(uut.getCoordinatesOfMembers(cluster, uut.findPossibleNodesToDeploy(cluster)), uut.requestTimeout)

        val s = stream[Int](pNames(0))
        val f1 = Filter1[Int](s, _ => true, Set())
        val f2 = Filter1[Int](f1, _ => true, Set())
        val result = uut.calculateVirtualPlacementWithCoords(cluster, f2, clientC, Map(pNames(0) -> publisher1), candidateCoordinates)
        val eps = 8.0
        assert(result(s).distance(new Coordinates(-75, 0, 0)) <= eps,"stream operator should be near publisher")
        assert(result(f1).distance(new Coordinates(-50, 0, 0)) <= eps, "filter 1 should be between publisher and client")
        assert(result(f2).distance(new Coordinates(-25, 0, 0)) <= eps, "filter 2 should be near client")
        //println("DEBUG \n " + result.map("\n" + _))
      }
      testConductor.enter("test calculateVirtualPlacementWithCoords() complete")
    }
  }


  "PietzuchAlgorithm - virtual and then physical placement" must {
    "return a valid mapping of a all operators to hosts" in {

      val uut = initUUT(cluster)
      val clientC = new Coordinates(50, 50, 0)
      val publisher1 = new Coordinates(0, 0, 0)
      val publisher2 = new Coordinates(0, 0, 0)
      val host1 = new Coordinates(50, 0, 0)
      val host2 = new Coordinates(25, 25, 0)
      setCoordinates(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val s = stream[Int](pNames(0))
        val f1 = Filter1[Int](s, _ => true, Set())
        val f2 = Filter1[Int](f1, _ => true, Set())
        val publishers = Map(pNames(0) -> getPublisherOn(cluster, getMembersWithRole(cluster, "Publisher").head, pNames(0)).get)
        val candidates = Await.result(uut.getCoordinatesOfMembers(cluster, uut.findPossibleNodesToDeploy(cluster)), uut.requestTimeout)
        val virtualCoordinates: Map[Query, Coordinates] = Await.result(uut.initialVirtualOperatorPlacement(cluster, f2), uut.requestTimeout)
        val physicalPlacements: Map[Query, Member] = virtualCoordinates.map(o => o._1 -> Await.result(uut.findHost(o._2, candidates, o._1, publishers.values.toList), uut.requestTimeout).member)
        assert(physicalPlacements.values.toSet.map((m: Member) => m.address).subsetOf(
          getMembersWithRole(cluster, "Candidate").map(c => c.address)),
          "all operators must be hosted on candidates")
        assert(!physicalPlacements.values.exists(_.address.port.get != 2504),
          "all operators should be deployed on host2 since it is right between client and publisher")
      }
      testConductor.enter("test initialVirtualOperatorPlacement and then physical placement complete")
    }
  }

  "PietzuchAlgorithm - BDP test" must {
    "produce a placement that has minimal BDP" in {

      val uut = initUUT(cluster)
      val clientC = new Coordinates(50, 50, 0)
      val publisher1 = new Coordinates(0, 0, 0) // DoorSensor
      val publisher2 = new Coordinates(0, 0, 0)
      val host1 = new Coordinates(50, 0, 0)
      val host2 = new Coordinates(25, 25, 0)
      setCoordinates(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {

        val s = stream[Int](pNames(0))
        val f1 = Filter1[Int](s, _ => true, Set())
        //val f2 = Filter1[Int](f1, _ => true, Set())
        val publishers = Map(pNames(0) -> getPublisherOn(cluster, getMembersWithRole(cluster, "Publisher").head, pNames(0)).get)
        val candidates = Await.result(uut.getCoordinatesOfMembers(cluster, uut.findPossibleNodesToDeploy(cluster)), uut.requestTimeout)
        // find all possible mappings of the entire query to the candidate nodes (hosts + client)
        val operators = List(s, f1)
        val h1 = candidates.head
        val h2 = candidates.last
        val perm: List[List[(Member, Coordinates)]] = candidates.toList.permutations.toList
        val permWithDuplicates = List(candidates.head, candidates.head) :: List(candidates.last, candidates.last) :: perm
        //println(s"\nDEBUG permutations: ${permWithDuplicates.map("\n" + _)}")

        val allPossibleMappings: List[Map[Query, (Member, Coordinates)]] = List(
          Map(s -> h1, f1 -> h1),
          Map(s -> h1, f1 -> h2),
          Map(s -> h2, f1 -> h1),
          Map(s -> h2, f1 -> h2)
        )
        val BDPs = allPossibleMappings.map(m => m -> getTestBDP(m, clientC, publisher1))
        val minBDP = BDPs.minBy(m => m._2)._2
        //println(s"\nDEBUG BDPs: ${BDPs.map(e => s"\n ${e._1.map(t => s"${t._1} -> ${t._2._2}")}  | ${e._2}")}")
        //println(s"\nDEBUG minimum BDP: $minBDP")

        uut.k = 1 // disable load-based host choice for repeatable tests
        val virtualCoords: Map[Query, Coordinates] = uut.calculateVirtualPlacementWithCoords(cluster, f1, clientC, Map(pNames(0) -> publisher1), candidates)
        val physicalPlacement: Map[Query, (Member, Coordinates)] = virtualCoords.map(vc => vc._1 -> Await.result(uut.findHost(vc._2, candidates, vc._1, publishers.values.toList), uut.requestTimeout))
          .map(e => e._1 -> (e._2.member, uut.getCoordinatesOfNodeBlocking(cluster, e._2.member)))
        val placementBDP = getTestBDP(physicalPlacement, clientC, publisher1)
        assert(placementBDP == minBDP, "BDP of placement must be minimum possible BDP of all possible placements")
      }

      testConductor.enter("test minimal BDP complete")
    }
  }
}
