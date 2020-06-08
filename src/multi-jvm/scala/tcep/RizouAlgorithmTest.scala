package tcep

import java.util.concurrent.TimeUnit

import akka.cluster.{Cluster, Member}
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries._
import tcep.dsl.Dsl.{Seconds => _, TimespanHelper => _, _}
import tcep.placement.mop.RizouAlgorithm
import tcep.placement.{PlacementStrategy, QueryDependencies}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

// need one concrete test class per node
class RizouMultiJvmNode1 extends RizouMultiNodeTestSpec
class RizouMultiJvmNode2 extends RizouMultiNodeTestSpec
class RizouMultiJvmClient extends RizouMultiNodeTestSpec
class RizouMultiJvmPublisher1 extends RizouMultiNodeTestSpec
class RizouMultiJvmPublisher2 extends RizouMultiNodeTestSpec

abstract class RizouMultiNodeTestSpec extends MultiJVMTestSetup {

  import TCEPMultiNodeConfig._

  val bdpAccuracy = 0.2
  val coordAccuracy = 6.0

  private def initUUT(cluster: Cluster): RizouAlgorithm.type = {
    val uut = RizouAlgorithm
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
    Await.result(coordUpdate, new FiniteDuration(10, TimeUnit.SECONDS))
  }

  "RizouAlgorithm - calculateVCSingleOperator() with two parents" must {
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
        val virtualCoords = Await.result(uut.calculateVCSingleOperator(cluster, op, Coordinates.origin, List(clientC, host1, host2), nnCandidates), uut.requestTimeout)
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

  "RizouAlgorithm - findHost" must {
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
        val candidateLoads = candidates.map(c => c._1 -> Await.result(uut.getLoadOfNode(cluster, c._1), uut.requestTimeout))
        assert(getMembersWithRole(cluster, "Candidate").contains(result.member) && !result.member.equals(getMembersWithRole(cluster, "Publisher").head),
          "must place operator among the three closest non-publisher nodes")
        assert(!candidateLoads.values.toSeq.sortWith(_ < _).take(uut.k).exists(l => l < candidateLoads(result.member)), "result must be the candidate with minimum load")
      }
    }
    testConductor.enter("test physical placement complete")
  }

  "RizouAlgorithm - extractOperators()" must {
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


  "RizouAlgorithm - calculateVirtualPlacementWithCoords() - one operator" must {
    "return the coordinate with minimal bdp for this single operator" in {

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
        val deps = List(publisher1, clientC)
        val resultBDP = calculateDistSum(deps, result(query))
        val approxBDP = findApproximateMinDistCoord(deps)
        assert(math.abs(resultBDP - approxBDP._2) <= bdpAccuracy)
        assert(result(query).distance(new Coordinates(-50, 0, 0)) <= coordAccuracy, "single operator should be placed between client and publisher")
      }
      testConductor.enter("test calculateVirtualPlacementWithCoords() with one operator complete")
    }
  }

  "RizouAlgorithm - calculateVirtualPlacementWithCoords() - binary operator query" must {
    "return the mapping of binary and stream operators to coordinates for the given query that minimizes total bdp" in {

      val uut = initUUT(cluster)
      val clientC = new Coordinates(100, 0, 0)
      val publisher1 = new Coordinates(0, 100, 0)
      val publisher2 = new Coordinates(0, -100, 0)
      val host1 = new Coordinates(-75, 0, 0)
      val host2 = new Coordinates(-50, 0, 0)
      setCoordinates(uut, clientC, publisher1, publisher2, host1, host2)

      runOn(client) {
        val candidateCoordinates = Await.result(uut.getCoordinatesOfMembers(cluster, uut.findPossibleNodesToDeploy(cluster)), uut.requestTimeout)
        val s1 = stream[Int](pNames(0))
        val s2 = stream[Int](pNames(1))
        val query = s1.and(s2)

        val result: Map[Query, Coordinates] = uut.calculateVirtualPlacementWithCoords(cluster, query, clientC, Map(pNames(0) -> publisher1, pNames(1) -> publisher2), candidateCoordinates)
            // since we use default bandwidth (using iperf for tests is not feasible), distance can be interpreted as bdp here
        val andDep = List(result(s1), result(s2), clientC)
        val s1Dep = List(publisher1, result(query))
        val s2Dep = List(publisher2, result(query))
        val resultBDPAnd = calculateDistSum(andDep, result(query))
        val resultBDPS1 = calculateDistSum(s1Dep, result(s1))
        val resultBDPS2 = calculateDistSum(s2Dep, result(s2))
        val approxBDPAnd = findApproximateMinDistCoord(andDep, 1000)
        val approxBDPS1 = findApproximateMinDistCoord(s1Dep, 1000) // coord cannot be used since |dependencies| <= |dim|
        val approxBDPS2 = findApproximateMinDistCoord(s2Dep, 1000)
        //println(s" result                       | approx                  | diff")
        //println(s" ${resultBDPAnd} ${result(query)} | ${approxBDPAnd._2} ${approxBDPAnd._1} | ${math.abs(resultBDPAnd - approxBDPAnd._2)}")
        //println(s" ${resultBDPS1} ${result(s1)} | ${approxBDPS1._2} ${approxBDPS1._1} | ${math.abs(resultBDPS1 - approxBDPS1._2)}")
        //println(s" ${resultBDPS2} ${result(s2)} | ${approxBDPS2._2} ${approxBDPS2._1} | ${math.abs(resultBDPS2 - approxBDPS2._2)}")

        assert(math.abs(resultBDPAnd - approxBDPAnd._2) <= bdpAccuracy, "BDP between AND and its dependencies must be close to the approximate minimum BDP")
        assert(math.abs(resultBDPS1 - approxBDPS1._2) <= bdpAccuracy, "BDP between stream 1 and its dependencies must be close to the approximate minimum BDP")
        assert(math.abs(resultBDPS2 - approxBDPS2._2) <= bdpAccuracy, "BDP between stream 2 and its dependencies must be close to the approximate minimum BDP")
        // pub1 (0, 100)
        //  \
        //  s1 (25, 50)
        //    \
        //     and (50, 0) -- client (100, 0)
        //    /
        //   s2 (25, -50)
        //  /
        // pub2 (0, -100)
        assert(result(query).distance(new Coordinates(50, 0, 0)) <= coordAccuracy)
        assert(result(s1).distance(new Coordinates(25, 50, 0)) <= coordAccuracy)
        assert(result(s2).distance(new Coordinates(25, -50, 0)) <= coordAccuracy)
      }
      testConductor.enter("test calculateVirtualPlacementWithCoords() with binary operator complete")
    }
  }

  "RizouAlgorithm - calculateVirtualPlacementWithCoords() - unary operator query" must {
    "return the mapping of operator to coordinates that has minimal bdp for the given unary operator query" in {

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
        // since this query contains only unary operators, their optimal coordinates are not unique (bandwidth is constant) -> only test for min bdp
        val f2Dep = List(result(f1), clientC)
        val f1Dep = List(result(s), result(f2))
        val sDep = List(publisher1, result(f1))
        val resultBDPf2 = calculateDistSum(f2Dep, result(f2))
        val resultBDPf1 = calculateDistSum(f1Dep, result(f1))
        val resultBDPs = calculateDistSum(sDep, result(s))
        val approxBDPf2 = findApproximateMinDistCoord(f2Dep, 1000)
        val approxBDPf1 = findApproximateMinDistCoord(f1Dep, 1000) // coord cannot be used since |dependencies| <= |dim|
        val approxBDPs = findApproximateMinDistCoord(sDep, 1000)

        assert(math.abs(resultBDPf2 - approxBDPf2._2) <= bdpAccuracy, "BDP between f2 and its dependencies must be close to the approximate minimum BDP")
        assert(math.abs(resultBDPf1 - approxBDPf1._2) <= bdpAccuracy, "BDP between f1 and its dependencies must be close to the approximate minimum BDP")
        assert(math.abs(resultBDPs - approxBDPs._2) <= bdpAccuracy, "BDP between stream  and its dependencies must be close to the approximate minimum BDP")

      }
      testConductor.enter("test calculateVirtualPlacementWithCoords() complete")
    }
  }


  "RizouAlgorithm - virtual and then physical placement" must {
    "return a valid mapping of a all operators to hosts" in {
      testConductor.enter("test initialVirtualOperatorPlacement start")

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
        val candidates = uut.findPossibleNodesToDeploy(cluster).map(c => c -> uut.getCoordinatesOfNodeBlocking(cluster, c)).toMap
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

  "RizouAlgorithm - BDP test" must {
    "produce a placement that has minimal BDP" in {
      testConductor.enter("test minimal BDP start")

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
        val candidates = uut.findPossibleNodesToDeploy(cluster).map(c => c -> uut.getCoordinatesOfNodeBlocking(cluster, c)).toMap
        // find all possible mappings of the entire query to the candidate nodes (hosts + client)
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
        val physicalPlacement: Map[Query, (Member, Coordinates)] = virtualCoords.map(vc => vc._1 -> uut.findHost(vc._2, candidates, vc._1, publishers.values.toList))
          .map(e => e._1 -> {
            val m = Await.result(e._2, uut.requestTimeout)
            (m.member, uut.getCoordinatesOfNodeBlocking(uut.cluster, m.member))
          })
        val placementBDP = getTestBDP(physicalPlacement, clientC, publisher1)
        assert(placementBDP == minBDP, "BDP of placement must be minimum possible BDP of all possible placements")
      }

      testConductor.enter("test minimal BDP complete")
    }
  }

  def calculateDistSum(dependencies: List[Coordinates], p: Coordinates): Double = dependencies.map(_.distance(p)).sum

  /**
    * approximates the coordinates for which the sum of distances to the dependencies becomes minimal
    * @param dependencies
    * @param samplesPerDimension number of samples to make per dimension
    * @return tuple of minimal coordinate and distance sum
    */
  def findApproximateMinDistCoord(dependencies: List[Coordinates], samplesPerDimension: Int = 100): (Coordinates, Double) = {

    if(samplesPerDimension > 1000) println("more than 1000 samples per dimensions, this can take a while...")
    if(dependencies.length < 3) println("number of dependencies is smaller than 3, note that there can be no unique solution for the coordinate in this case")
    val startTime = System.currentTimeMillis()

    val minX = dependencies.minBy(_.x).x
    val minY = dependencies.minBy(_.y).y
    val maxX = dependencies.maxBy(_.x).x
    val maxY = dependencies.maxBy(_.y).y
    val stepSizeX: Double = if(maxX != minX) math.abs(maxX - minX) / samplesPerDimension else 1.0
    val stepSizeY: Double = if(maxY != minY) math.abs(maxY - minY) / samplesPerDimension else 1.0
    val allCoords = (minX to maxX by stepSizeX).flatMap(x => (minY to maxY by stepSizeY).map(y => new Coordinates(x, y, 0)))
    val allDists = allCoords.map(c => (c, calculateDistSum(dependencies, c)))
    val minDist = allDists.minBy(_._2)
    //println(s"calculation took ${System.currentTimeMillis() - startTime}ms, stepSizes: ($stepSizeX, $stepSizeY)")
    minDist
  }
}
