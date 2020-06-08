package tcep.simulation.tcep

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorRef, Address, CoordinatedShutdown, RootActorPath}
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.{Member, MemberStatus}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import tcep.data.Queries._
import tcep.data.Structures.MachineLoad
import tcep.dsl.Dsl._
import tcep.graph.QueryGraph
import tcep.graph.nodes.traits.{TransitionConfig, TransitionExecutionModes, TransitionModeNames}
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.machinenodes.consumers.Consumer.SetStreams
import tcep.machinenodes.helper.actors._
import tcep.placement.manets.StarksAlgorithm
import tcep.placement.mop.RizouAlgorithm
import tcep.placement.sbon.PietzuchAlgorithm
import tcep.placement.vivaldi.VivaldiCoordinates
import tcep.placement.{GlobalOptimalBDPAlgorithm, MobilityTolerantAlgorithm, PlacementStrategy, RandomAlgorithm}
import tcep.publishers.Publisher.StartStreams
import tcep.utils.{SizeEstimator, SpecialStats, TCEPUtils}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class SimulationSetup(directory: Option[File], mode: Int, transitionMode: TransitionConfig, durationInMinutes: Option[Int] = None, startingPlacementAlgorithm: String = PietzuchAlgorithm.name, overridePublisherPorts: Option[Set[Int]] = None, queryString: String = "AccidentDetection", eventRate: String = "1", fixedSimulationProperties: Map[Symbol, Int] = Map(), mapek: String = "requirementBased", requirementStr: String = "latency") extends VivaldiCoordinates with ActorLogging {



  type StreamData = MobilityData
  val transitionTesting = false
  val isMininetSim: Boolean = ConfigFactory.load().getBoolean("constants.mininet-simulation")
  val nSpeedPublishers = ConfigFactory.load().getInt("constants.number-of-speed-publisher-nodes")
  val nSections = ConfigFactory.load().getInt("constants.number-of-road-sections")
  val minNumberOfMembers = ConfigFactory.load().getInt("akka.cluster.min-nr-of-members")
  val publisherBasePort = ConfigFactory.load().getInt("constants.base-port") + 1
  val densityPublisherNodePort = publisherBasePort + nSpeedPublishers // base port to distinguish densityPublisher nodes from SpeedPublishers -> last publisher is density publisher
  val speedPublisherNodePorts = publisherBasePort until publisherBasePort + nSpeedPublishers
  // publisher naming convention: P:hostname:port, e.g. "P:p1:2502", or "P:localhost:2501" (for local testing)
  var publishers: Map[String, ActorRef] = Map.empty[String, ActorRef]
  val publisherPorts: Set[Int] = if(overridePublisherPorts.isDefined && overridePublisherPorts.get.size >= 2) overridePublisherPorts.get // for unit/integration testing
                       else densityPublisherNodePort :: speedPublisherNodePorts.toList toSet
  var consumers: ListBuffer[ActorRef] = ListBuffer.empty[ActorRef]
  var taskManagerActorRefs: Map[Member, ActorRef] = Map()
  val minimumBandwidthMeasurements = if(isMininetSim) 0 else minNumberOfMembers * (minNumberOfMembers - 1)   // n * (n-1) measurements between n nodes
  var bandwidthMeasurementsStarted = false
  var measuredLinks: mutable.Map[(Address, Address), Double] = mutable.Map[(Address, Address), Double]()
  var bandwidthMeasurementsCompleted = 0
  var coordinatesEstablishedCount = 0
  var coordinatesEstablished = false
  var publisherActorRefsBroadcastComplete = false
  var publisherActorBroadcastAcks = 0
  var simulationStarted = false
  val defaultDuration: Long = ConfigFactory.load().getLong("constants.simulation-time")
  val startTime = System.currentTimeMillis()
  val totalDuration = if(durationInMinutes.isDefined) FiniteDuration(durationInMinutes.get, TimeUnit.MINUTES) else FiniteDuration(defaultDuration, TimeUnit.MINUTES)
  val startDelay = new FiniteDuration(5, TimeUnit.SECONDS)
  val samplingInterval = new FiniteDuration(1, TimeUnit.SECONDS)
  val requirementChangeDelay = new FiniteDuration(10, TimeUnit.SECONDS)
  val ws = slidingWindow(1.seconds) // join window size
  val latencyRequirement = latency < timespan(1100.milliseconds) otherwise None
  val messageHopsRequirement = hops < 3 otherwise None
  val loadRequirement = load < MachineLoad(10.0d) otherwise None
  val frequencyRequirement = frequency > Frequency(50, 5) otherwise None
  var graphs: Map[Int, QueryGraph] = Map()
  //val query = ConfigFactory.load().getStringList("constants.query")

  log.info(s"publisher ports: speed ($speedPublisherNodePorts) density: $densityPublisherNodePort \n $publisherPorts")
  lazy val speedPublishers: Vector[(String, ActorRef)] = publishers.toVector.filter(p => speedPublisherNodePorts.contains(p._2.path.address.port.getOrElse(-1)))
  def speedStreams(nSpeedStreamOperators: Int): Vector[Stream1[StreamData]] = {
    log.info(s"speedPublishers: ${speedPublishers.size} for $nSpeedStreamOperators operators \n ${speedPublishers.mkString("\n")}")
    // enough speed publisher nodes for the query
    if(speedPublishers.size >= nSpeedStreamOperators)
      speedPublishers.map(p => Stream1[MobilityData](p._1, Set())).sortBy(_.publisherName)
    // fewer speed publisher nodes than needed for the query
    else if(speedPublishers.nonEmpty) {
      log.warning(s"not enough speed publishers for all stream operators, reusing some (${speedPublishers.size} of $nSpeedStreamOperators")
      0 until nSpeedStreamOperators map { i => Stream1[MobilityData](speedPublishers(i % speedPublishers.size)._1, Set())} toVector
    }
    else throw new IllegalArgumentException(s"no speed publishers found, publishers: \n $publishers")
  }
  def densityPublishers: Vector[(String, ActorRef)] = publishers.toVector.filter(_._2.path.address.port.getOrElse(-1) >= densityPublisherNodePort)
  def singlePublisherNodeDensityStream(section: Int): Stream1[Int] = {
    val sectionDensityPublisher = densityPublishers.find(_._2.path.name.split("-").last.contentEquals(section.toString))
      .getOrElse(throw new IllegalArgumentException(s"density publisher actor for section $section not found among $densityPublishers"))
    val dp = Stream1[Int](sectionDensityPublisher._1, Set())
    log.info(s"using density publisher actor ${sectionDensityPublisher._2 } for section $section from a single node")
    dp
  }

  def getStreams(): /*(Vector[Stream1[MobilityData]],Vector[Stream1[Int]])*/ Seq[Any] = {
    log.info("Creating Streams!")
    val streams = mode match {
      case Mode.MADRID_TRACES =>
        val dense = 0 to nSections-1 map (section => {
          this.singlePublisherNodeDensityStream(section)
        })
        //(this.speedStreams, dense.toVector)
        Seq(this.speedStreams(12), dense.toVector)
      case Mode.LINEAR_ROAD =>
        val linearRoadPublishers = publishers.toVector
        //linearRoadPublishers.map(p => Stream1[LinearRoadData](p._1, Set())).sortBy(_.publisherName)
        val publisherVector = linearRoadPublishers.map(p => Stream1[LinearRoadDataNew](p._1, Set())).sortBy(_.publisherName)
        Seq(publisherVector)
      case Mode.YAHOO_STREAMING =>
        val yahooPublishers = publishers.toVector
        val pubVector = yahooPublishers.map(p => Stream1[YahooDataNew](p._1, Set())).sortBy(_.publisherName)
        Seq(pubVector)

      case _ =>
        val dense = 0 to nSections-1 map (section => {
          this.singlePublisherNodeDensityStream(section)
        })
        Seq(this.speedStreams(12), dense.toVector)
    }
    log.info(s"Streams are: ${streams}")
    streams
  }

  override def receive: Receive = {
    super.receive orElse {

      case BandwidthMeasurementComplete(source: Address, target: Address, bw: Double) =>
        //if(!measuredLinks.contains((target, source)))
        measuredLinks += (source, target) -> bw
        bandwidthMeasurementsCompleted += 1
        log.info(s"${System.currentTimeMillis() - startTime}ms passed, received bandwidthMeasurementComplete message from $source to $target: $bw")

      case SetMissingBandwidthMeasurementDefaults => // called as a fallback for missing bandwidth measurements 3 minutes after starting measurements
        val defaultRate = ConfigFactory.load().getDouble("constants.default-data-rate")
        cluster.state.members.flatMap(m => cluster.state.members.filter(m != _).map(o => (m.address, o.address)))
          .foreach(pair => measuredLinks.getOrElseUpdate(pair, defaultRate))

      case VivaldiCoordinatesEstablished() =>
        coordinatesEstablishedCount += 1
        if (coordinatesEstablishedCount >= minNumberOfMembers) {
          log.info("all nodes established their coordinates")
          coordinatesEstablished = true
        }

      case SetPublisherActorRefsACK() =>
        publisherActorBroadcastAcks += 1
        if(publisherActorBroadcastAcks >= minNumberOfMembers) {
          log.info("all nodes received the publisher actorRefs")
          publisherActorRefsBroadcastComplete = true
        }

      case TaskManagerFound(member, ref) =>
        log.info(s"found taskManager on node $member, ${taskManagerActorRefs.size} of $minNumberOfMembers")
        taskManagerActorRefs = taskManagerActorRefs.updated(member, ref)
        if(taskManagerActorRefs.size >= minNumberOfMembers) {
          taskManagerActorRefs.foreach(_._2 ! AllTaskManagerActors(taskManagerActorRefs))
        }

      case s: CurrentClusterState => this.currentClusterState(s)
      case _ =>
    }
  }


  // accident detection query: low speed, high density in section A, high speed, low density in following road section B
  def accidentQuery(requirements: Requirement*) = {

    val sections = 0 to 1 map(section => { // careful when increasing number of subtrees (21 operators per tree) -> too many operators make transitions with MDCEP unreliable
      val avgSpeed = averageSpeedPerSectionQuery(section)
      val density = singlePublisherNodeDensityStream(section)
      //val and = avgSpeed.join(density, slidingWindow(1 instances), slidingWindow(1 instances))
      val and = avgSpeed.and(density)
      val filter = and.where(
        (_: MobilityData, _: Int) => true /*(sA: Double, dA: Double) => sA <= 60 && dA >= 10*/ // replace with always true to keep events (and measurements) coming in
      )
      filter
    })
    val and01 = sections(0).and(sections(1), requirements: _*)
    // and12 = sections(1).and(sections(2), requirements: _*)
    and01
  }

  // average speed per section (average operator uses only values matching section)
  def averageSpeedPerSectionQuery(section: Int, requirements: Requirement*) = {
    val nSpeedStreamOperators = 8
    val streams = this.speedStreams(nSpeedStreamOperators)
    if(streams.size < nSpeedStreamOperators) log.warning(s"simulation should have $nSpeedStreamOperators publishers, actual: ${streams.size}; attempting to re-use publishers to generate $nSpeedStreamOperators streams")
    val and01 = streams(0).and(streams(1))
    val and23 = streams(2).and(streams(3))
    val and0123 = and01.and(and23)
    val averageA = Average4(and0123, sectionFilter = Some(section))

    val and67 = streams(4).and(streams(5))
    val and89 = streams(6).and(streams(7))
    val and6789 = and67.and(and89)
    val averageB = Average4(and6789, sectionFilter = Some(section))

    val andAB = averageA.and(averageB)
    val averageAB = Average2(andAB, Set(requirements: _*), sectionFilter = Some(section))
    averageAB
  }

  // average speed of all publishers
  def averageSpeedQuery(requirements: Requirement*) = {
    val nSpeedStreamOperators = 8
    val streams = this.speedStreams(nSpeedStreamOperators)
    if(streams.size < nSpeedStreamOperators) log.warning(s"simulation should have $nSpeedStreamOperators publishers, actual: ${streams.size}; attempting to re-use publishers to generate $nSpeedStreamOperators streams")
    val and01 = streams(0).and(streams(1))
    val and23 = streams(2).and(streams(3))
    val and0123 = and01.and(and23)
    val averageA = Average4(and0123)

    val and67 = streams(4).and(streams(5))
    val and89 = streams(6).and(streams(7))
    val and6789 = and67.and(and89)
    val averageB = Average4(and6789)

    val andAB = averageA.and(averageB)
    val averageAB = Average2(andAB, Set(requirements: _*))
    averageAB
  }

  override def preStart(): Unit = {
    super.preStart()
    context.system.scheduler.scheduleOnce(new FiniteDuration(5, TimeUnit.SECONDS))(checkAndRunQuery)
  }

  override def currentClusterState(state: CurrentClusterState): Unit = {
    super.currentClusterState(state)
    log.info("received currentClusterState, extracting publishers")
    state.members.filter(x => x.status == MemberStatus.Up && x.hasRole("Publisher")) foreach extractProducers
  }

  override def memberUp(member: Member): Unit = {
    super.memberUp(member)
    if (member.hasRole("Publisher")) extractProducers(member)
    else {
      if (member.hasRole("Consumer")) {
        implicit val timeout = Timeout(20, TimeUnit.SECONDS)
        val subref = for {
          actorRef <- context.actorSelection(RootActorPath(member.address) / "user" / "consumer").resolveOne()
        } yield {
          actorRef
        }
        subref.onComplete {
          case Success(actorRef) =>
            log.info(s"SUBSCRIBER found: ${actorRef.path}")
            this.consumers += actorRef
          case Failure(exception) =>
            log.info(s"Failed to get SUBSCRIBER cause of $exception")
        }
      }
      else
        log.info(s"found member $member with role: ${member.roles}")
    }
  }

  // finds publisher actors (speed and density) on publisher nodes and saves their ActorRef
  def extractProducers(member: Member): Unit = {
    log.info(s"Found publisher node ${member}")
    implicit val timeout = Timeout(10, TimeUnit.SECONDS)
    if(member.address.port.getOrElse(-1) >= densityPublisherNodePort) { // densityPublisher node -> multiple publisher actors
      0 until nSections foreach { i =>
        if (!publishers.keys.exists(_.contains(s"densityPublisher-$i-"))) {
          val publisherActorRef = for {ref <- context.actorSelection(RootActorPath(member.address) / "user" / s"P:*-densityPublisher-$i-*").resolveOne()} yield ref
          publisherActorRef.onComplete {
            case Success(ref) =>
              val name = ref.toString.split("/").last.split("#").head
              log.info(s"saving publisher ${ref} as $name")
              this.savePublisher(name -> ref)
            case Failure(exception) =>
              log.warning(s"failed to resolve publisher actor on member $member, due to ${exception.getMessage}, retrying")
              extractProducers(member)
          }
        }
      }
    } else {
        val resolvePublisher = for {
          publisherActorRef <- context.actorSelection(RootActorPath(member.address) / "user" / "P:*").resolveOne() // speedPublisher node -> only one publisher actor
        } yield {
          publisherActorRef
        }
        resolvePublisher.onComplete {
          case Success(publisherActorRef) =>
            val name = publisherActorRef.toString.split("/").last.split("#").head
            log.info(s"saving publisher ${publisherActorRef} as $name")
            this.savePublisher(name -> publisherActorRef)
            log.info(s"current publishers: \n ${publishers.keys}")
          case Failure(exception) =>
            log.warning(s"failed to resolve publisher actor on member $member, due to ${exception.getMessage}, retrying")
            extractProducers(member)
        }
      }
  }

  def savePublisher(publisher: (String, ActorRef)) = publishers.synchronized { // avoid insert being lost when both resolve futures complete at the same time
    publishers += publisher
  }

  //If
  // 1. all publishers required in the query are available and the minimum number of members is up
  // 2. all link bandwidths are measured
  // 3. all nodes have established their coordinates
  // ... then run the simulation
  def checkAndRunQuery(): Unit = {

    val timeSinceStart = System.currentTimeMillis() - startTime
    val upMembers = cluster.state.members.filter(m => m.status == MemberStatus.up && !m.hasRole("VivaldiRef"))
    val foundPublisherPorts = publishers.values.map(_.path.address.port.getOrElse(-1)).toSet
    log.info(s"checking if ready, time since init: ${timeSinceStart}ms," +
      s" taskManagers found: ${taskManagerActorRefs.size} of $minNumberOfMembers," +
      s" completed bandwidthMeasurements: (${measuredLinks.size} of $minimumBandwidthMeasurements)," +
      s" upMembers: (${upMembers.size} of $minNumberOfMembers), (joining or other state: ${cluster.state.members.size}), " +
      s" publishers: ${publishers.keySet.size} of ${nSpeedPublishers + nSections}, missing publisher port numbers: ${publisherPorts.diff(foundPublisherPorts)}" +
      s" coordinates established: ${coordinatesEstablished} " +
      s" actorrefbroadcast complete: ${publisherActorRefsBroadcastComplete} " +
      s" num of subscribers: ${consumers.size} " +
      s" started: $simulationStarted")

    if(taskManagerActorRefs.size < minNumberOfMembers) {
      for {
        pairs <- upMembers.map(m => for { ref <- TCEPUtils.getTaskManagerOfMember(cluster, m) } yield (m, ref))
      } yield pairs.onComplete {
        case Success(pair) => self ! TaskManagerFound(pair._1, pair._2)
        case _ =>
      }
    } else if(upMembers.size >= minNumberOfMembers && !bandwidthMeasurementsStarted) { // notify all TaskManagers to start bandwidth measurements once all nodes are up
      upMembers.foreach(m => TCEPUtils.selectTaskManagerOn(cluster, m.address) ! InitialBandwidthMeasurementStart())
      context.system.scheduler.scheduleOnce(FiniteDuration(3, TimeUnit.MINUTES), self, SetMissingBandwidthMeasurementDefaults)
      bandwidthMeasurementsStarted = true
    }

    if (measuredLinks.size >= minimumBandwidthMeasurements) {
      cluster.state.members.foreach(m => TCEPUtils.selectDistVivaldiOn(cluster, m.address) ! StartVivaldiUpdates())
      // broadcast publisher actorRefs to all nodes so that when transitioning, not every placement algorithm instance has to retrieve them for themselves
    }
    // distinguish publishers by ports since they're easier to generalize and set up for mininet, geni and testing than hostnames
    if(publisherPorts.subsetOf(foundPublisherPorts) && taskManagerActorRefs.size >= minNumberOfMembers && publishers.size >= nSpeedPublishers + nSections)
      upMembers.foreach(m => TCEPUtils.selectTaskManagerOn(cluster, m.address) ! SetPublisherActorRefs(publishers))

    // publishers found (speed publisher nodes with 1 speed publisher actors, 1 density publisher node with nSections densityPublisher actors
    if (publisherPorts.subsetOf(foundPublisherPorts) && publishers.size >= nSpeedPublishers + nSections &&
      upMembers.size >= minNumberOfMembers && // nodes up
      taskManagerActorRefs.size >= minNumberOfMembers && // taskManager ActorRefs have been found and broadcast to all other taskManagers      measuredLinks.size >= minimumBandwidthMeasurements && // all links bandwidth measured
      coordinatesEstablished && // all nodes have established their coordinates
      publisherActorRefsBroadcastComplete &&
      consumers.size >= 1 &&
      !simulationStarted
    ){
      simulationStarted = true
      SpecialStats.debug(s"$this", s"ready to start after $timeSinceStart, setting up simulation...")
      log.info(s"measured bandwidths: \n ${measuredLinks.toList.sorted.map(e => s"\n ${e._1._1} <-> ${e._1._2} : ${e._2} Mbit/s")}")
      log.info(s"telling publishers to start streams...")
      publishers.foreach(p => p._2 ! StartStreams())
      this.consumers.last ! SetStreams(this.getStreams())
      this.executeSimulation()

    } else {
      SpecialStats.debug(s"$this", s"$timeSinceStart ms since start, waiting for initialization to finish; " +
        s"measured links: (${measuredLinks.size} of $minimumBandwidthMeasurements);" +
        s"coordinates established: ($coordinatesEstablishedCount of $minNumberOfMembers); " +
        s"publishers: ${publishers.mkString(";")}" +
        s"publisher actorRefs broadcast: $publisherActorBroadcastAcks of $minNumberOfMembers " +
        s"Subscribers are $consumers")
      context.system.scheduler.scheduleOnce(new FiniteDuration(5, TimeUnit.SECONDS))(checkAndRunQuery)
    }
  }

  /**
    * run a simulation with the specified starting parameters
    * @param i                  simulation run number
    * @param placementStrategy  starting placement strategy
    * @param transitionConfig     transition mode (MFGS or SMS) to use if requirements change
    * @param initialRequirements query to be executed
    * @param finishedCallback   callback to apply when simulation is finished
    * @param requirementChanges  requirement to change to after requirementChangeDelay
    * @return queryGraph of the running simulation
    */
  def runSimulation(i: Int, placementStrategy: PlacementStrategy, transitionConfig: TransitionConfig, finishedCallback: () => Any,
                    initialRequirements: Set[Requirement], requirementChanges: Option[Set[Requirement]] = None,
                    queryStr: String = queryString): QueryGraph = {

      val query = queryStr match {
      case "Stream" => stream[StreamData](speedPublishers(0)._1, initialRequirements.toSeq: _*)
      case "Filter" => stream[StreamData](speedPublishers(0)._1, initialRequirements.toSeq: _*).where(_ => true)
      case "Conjunction" => stream[StreamData](speedPublishers(0)._1).and(stream[StreamData](speedPublishers(1)._1), initialRequirements.toSeq: _*)
      case "Disjunction" => stream[StreamData](speedPublishers(0)._1).or(stream[StreamData](speedPublishers(1)._1), initialRequirements.toSeq: _*)
      case "Join" => stream[StreamData](speedPublishers(0)._1).join(stream[StreamData](speedPublishers(1)._1), slidingWindow(5.seconds), slidingWindow(5.seconds)).where((_, _) => true, initialRequirements.toSeq: _*)
      case "SelfJoin" => stream[StreamData](speedPublishers(0)._1).selfJoin(slidingWindow(5.seconds), slidingWindow(5.seconds), initialRequirements.toSeq: _*)
      case "AccidentDetection" => accidentQuery(initialRequirements.toSeq: _*)
    }

        //accidentQuery(initialRequirements.toSeq: _*)
      //val query = Average2(speedStreams(0).and(speedStreams(1)), initialRequirements)
      //val query = averageSpeedQuery(initialRequirements.toSeq: _*)
      def debugQuery(parent: Query1[MobilityData] = speedStreams(1)(0), depth: Int = 200, count: Int = 0): Query1[MobilityData] = {
        if(count < depth) debugQuery(parent.where(_ => true), depth, count + 1)
        else parent.where(_ => true, initialRequirements.toSeq :_*)
      }
      def debugQueryTree(depth: Int = 3, count: Int = 0): Average2[MobilityData] = {
        if(count < depth) Average2(debugQueryTree(depth, count + 1).and(debugQueryTree(depth, count + 1)))
        else averageSpeedQuery() // 18 operators
      }
      log.info(s"starting $transitionMode ${placementStrategy.name} algorithm simulation number $i with requirementChanges $requirementChanges \n and query $query")

      val sim = new Simulation(cluster, name = s"${transitionMode}-${placementStrategy.name}-${queryStr}-${mapek}-${requirementStr}-$i", directory = directory,
        query = query, transitionConfig = transitionConfig, publishers = publishers, consumer = consumers.last,
        startingPlacementStrategy = Some(placementStrategy), allRecords = AllRecords(), context = this.context,
        mapekType = this.mapek)
      var percentage: Double = i*2d
      val graph = sim.startSimulation(percentage, placementStrategy.name, queryStr, eventRate, startDelay, samplingInterval, totalDuration)(finishedCallback) // (start simulation time, interval, end time (s))
      graphs = graphs.+(i - 1 -> graph)
      context.system.scheduler.scheduleOnce(totalDuration)(this.shutdown())
      if (requirementChanges.isDefined && requirementChanges.get.nonEmpty) {
        val firstDelay = requirementChangeDelay.+(FiniteDuration(40, TimeUnit.SECONDS))
        log.info(s"scheduling first requirement change after $firstDelay for graph ${graphs(i - 1)}")
        context.system.scheduler.scheduleOnce(firstDelay)(changeReqTask(i, initialRequirements.toSeq, requirementChanges.get.toSeq))
        //log.info(s"scheduling second requirement change after ${requirementChangeDelay.mul(2)} for graph ${graphs(i-1)}")
        //context.system.scheduler.scheduleOnce(requirementChangeDelay.mul(2))(changeReqTask(i, requirementChanges.get.toSeq, initialRequirements.toSeq))

        // iterates over all available algorithms (-> 1 transition every requirementChangeDelay)
        if (transitionTesting) {
          val allAlgorithms = ConfigFactory.load().getStringList("benchmark.general.algorithms").asScala
          //val allAlgorithms = List(PietzuchAlgorithm.name, GlobalOptimalBDPAlgorithm.name)
          var mult = 2
          val repetitions: Double = (totalDuration.-(firstDelay).div(requirementChangeDelay) - 2) / allAlgorithms.size
          for (repeat <- 0 until repetitions.toInt) {
            allAlgorithms.foreach(a => {
              val t = firstDelay.+(requirementChangeDelay.mul(mult))
              context.system.scheduler.scheduleOnce(t)(explicitlyChangeAlgorithmTask(i, a))
              mult += 1
              log.info(s"scheduling manual algorithm change to $a after $t ")
            })
          }
        }
      }
      graph
  }

  def changeReqTask(i: Int, oldReq: Seq[Requirement], newReq: Seq[Requirement]): Unit = {
    log.info(s"running scheduled req change for graph $i from oldReq: $oldReq to newReq: $newReq")
    graphs(i - 1).removeDemand(oldReq)
    graphs(i - 1).addDemand(newReq)
  }

  def explicitlyChangeAlgorithmTask(i: Int, algorithm: String): Unit = {
    log.info(s"executing scheduled explicit change algorithm of graph $i to $algorithm")
    graphs(i - 1).manualTransition(algorithm)
  }

  // interactive simulation mode: select query and algorithm via GUI at runtime
  def testGUI(i: Int, j: Int, windowSize: Int): Unit = {
    try {
      var mfgsSims: mutable.MutableList[Simulation] = mutable.MutableList()
      var graphs: mutable.MutableList[QueryGraph] = mutable.MutableList()
      var currentTransitionMode: String = null
      var currentStrategyName: String = null
      var currentQuery: String = null
      @volatile var simulationStarted = false
      val allReqs = Set(latencyRequirement, loadRequirement, messageHopsRequirement)

      val startSimulationRequest = (transitionMode: String, strategyName: String, optimizationCriteria: List[String], query: String) => {
        if (simulationStarted) {
          return
        }
        simulationStarted = true
        val mode = transitionMode match {
          case "MFGS" => TransitionModeNames.MFGS
          case "SMS" => TransitionModeNames.SMS
        }
        var globalOptimalBDPQuery: Query = null

        val newReqs =
          if (optimizationCriteria != null) optimizationCriteria.map(reqName => allReqs.find(_.name == reqName).getOrElse(throw new IllegalArgumentException(s"unknown requirement name: ${reqName}")))
          else {
            Seq(latencyRequirement, messageHopsRequirement)
          }
        log.debug(s"-----------------------------")
        log.debug(s"strategy name: ${strategyName}")
        log.debug(s"-----------------------------")
        log.info(s"-----------------------------")
        log.info(s"strategy name: ${strategyName}")
        log.info(s"-----------------------------")

        //globalOptimalBDPQuery = stream[Int](pNames(0)).join(stream[Int](pNames(1)), slidingWindow(windowSize.seconds), slidingWindow(windowSize.seconds)).where((_, _) => true, newReqs:_*)
        globalOptimalBDPQuery = query match {
          case "Stream" => stream[StreamData](speedPublishers(0)._1, newReqs: _*)
          case "Filter" => stream[StreamData, StreamData](speedPublishers(0)._1, newReqs: _*).where(_.speed >= _.speed)
          case "Conjunction" => stream[StreamData](speedPublishers(0)._1).and(stream[StreamData](speedPublishers(1)._1), newReqs: _*)
          case "Disjunction" => stream[StreamData](speedPublishers(0)._1).or(stream[StreamData](speedPublishers(1)._1), newReqs: _*)
          case "Join" => stream[StreamData](speedPublishers(0)._1).join(stream[StreamData](speedPublishers(1)._1), slidingWindow(windowSize.seconds), slidingWindow(windowSize.seconds)).where((_, _) => true, newReqs: _*)
          case "SelfJoin" => stream[StreamData](speedPublishers(0)._1).selfJoin(slidingWindow(windowSize.seconds), slidingWindow(windowSize.seconds), newReqs: _*)
          case "AccidentDetection" => accidentQuery(newReqs: _*)
        }

        var percentage: Double = i * 2d
        val mfgsSim = new Simulation(cluster, name = transitionMode, directory = directory, query = globalOptimalBDPQuery, TransitionConfig(mode, TransitionExecutionModes.CONCURRENT_MODE), publishers = publishers, consumers.last, startingPlacementStrategy = Some(PlacementStrategy.getStrategyByName(strategyName)), allRecords = AllRecords(), fixedSimulationProperties = fixedSimulationProperties, context = this.context)
        mfgsSims += mfgsSim

        //start at 20th second, and keep recording data for 5 minutes
        val graph = mfgsSim.startSimulation(percentage, s"$transitionMode-Strategy", query, eventRate, startDelay, samplingInterval, FiniteDuration.apply(0, TimeUnit.SECONDS))(null)
        graphs += graph

        currentStrategyName = graph.getPlacementStrategy()
        currentTransitionMode = transitionMode
        currentQuery = query
      }

      val transitionRequest = (optimizationCriteria: List[String]) => {
        log.info("Received new optimization criteria " + optimizationCriteria.toString())
        //optimizationCriteria.foreach(op => {
        graphs.foreach(graph => {
          val newReqs = optimizationCriteria.map(reqName => allReqs.find(_.name == reqName).getOrElse(throw new IllegalArgumentException(s"unknown requirement name: ${reqName}")))
          graph.removeDemand(allReqs.toSeq)
          graph.addDemand(newReqs)
        })
      }

      val manualTransitionRequest = (algorithmName: String) => {
        log.info("Received new manual transition request " + algorithmName)
        graphs.foreach(graph => {
          graph.manualTransition(algorithmName)
        })
      }

      val stop = () => {
        mfgsSims.foreach(sim => {
          sim.stopSimulation()
        })
        graphs.foreach(graph => {
          graph.stop()
        })
        graphs = mutable.MutableList()
        mfgsSims = mutable.MutableList()
        simulationStarted = false
      }

      val status = () => {
        var strategyName = "none"
        var transitionMode = "none"
        var query = "none"
        if (currentStrategyName != null) {
          strategyName = currentStrategyName
        }
        if (currentTransitionMode != null) {
          transitionMode = currentTransitionMode
        }

        if (currentQuery != null) {
          query = currentQuery
        }

        if (graphs.nonEmpty) {
          val graph = graphs.get(0) // we only get the strategy from one graph assuming that all of them execute the same strategy
          currentStrategyName = graph.get.getPlacementStrategy()
        }

        val response = Map(
          "placementStrategy" -> strategyName,
          "transitionMode" -> transitionMode,
          "query" -> query
        )
        print(response)
        response
      }

      val server = new TCEPSocket(this.context.system)
      server.startServer(startSimulationRequest, transitionRequest, manualTransitionRequest, stop, status)
      log.info(s"started GUI server $server for interactive simulation")
    } catch {
      case e: Throwable =>
        log.error(e, "testGUI crashed, restarting...")
        testGUI(i, j, windowSize)
    }
  }


  def executeSimulation(): Unit = try {

    val reqChange = requirementStr match {
      case "latency" => latencyRequirement
      case "load" => loadRequirement
      case "hops" => messageHopsRequirement
      case _ =>
        log.warning(s"unknown initial requirement supplied as argument: $requirementStr, defaulting to latencyReq")
        latencyRequirement
    }

    mode match {

      case Mode.TEST_RELAXATION => for (i <- Range(1, 50))
        this.runSimulation(i, PietzuchAlgorithm, transitionMode,
          () => log.info(s"Starks algorithm Simulation Ended for $i"),
          Set(latencyRequirement))

      case Mode.TEST_STARKS => for (i <- Range(1, 50))
        this.runSimulation(i, StarksAlgorithm, transitionMode,
          () => log.info(s"Starks algorithm Simulation Ended for $i"),
          Set(messageHopsRequirement))

      case Mode.TEST_RIZOU => for (i <- Range(1, 50))
        this.runSimulation(i, RizouAlgorithm, transitionMode,
          () => log.info(s"Starks algorithm Simulation Ended for $i"),
          Set(loadRequirement)) // use load requirement to make it "selectable" for RequirementBasedMAPEK/BenchmarkingNode

      case Mode.TEST_PC => for (i <- Range (1, 50) )
        this.runSimulation (i,  MobilityTolerantAlgorithm, transitionMode,
          () => log.info (s"Producer Consumer algorithm Simulation Ended for $i"),
          Set(latencyRequirement), Some(Set()))

      case Mode.TEST_OPTIMAL => for (i <- Range (1, 50) )
        this.runSimulation (i, GlobalOptimalBDPAlgorithm, transitionMode,
          () => log.info (s"Producer Consumer algorithm Simulation Ended for $i"),
          Set(latencyRequirement), Some(Set()))

      case Mode.TEST_RANDOM => for (i <- Range (1, 50) )
        this.runSimulation (i, RandomAlgorithm, transitionMode,
          () => log.info (s"Producer Consumer algorithm Simulation Ended for $i"),
          Set(latencyRequirement), Some(Set()))

      case Mode.TEST_SMS =>
        this.runSimulation(1, PlacementStrategy.getStrategyByName(startingPlacementAlgorithm), TransitionConfig(TransitionModeNames.SMS, TransitionExecutionModes.CONCURRENT_MODE),
          () => log.info(s"SMS transition $startingPlacementAlgorithm algorithm Simulation ended"),
          Set(latencyRequirement), Some(Set(messageHopsRequirement)))

      case Mode.TEST_MFGS =>
        this.runSimulation(1, PlacementStrategy.getStrategyByName(startingPlacementAlgorithm), TransitionConfig(TransitionModeNames.MFGS, TransitionExecutionModes.SEQUENTIAL_MODE),
          () => log.info(s"MFGS transition $startingPlacementAlgorithm algorithm Simulation ended"),
          Set(latencyRequirement), Some(Set(reqChange)))

      case Mode.TEST_GUI => this.testGUI(0, 0, 5)
      case Mode.DO_NOTHING => log.info("simulation ended!")
      case Mode.TEST_LIGHTWEIGHT =>
        this.runSimulation(1, PlacementStrategy.getStrategyByName(startingPlacementAlgorithm), transitionMode,
          () => log.info(s"Lightweight transitions simulation ended"),
          Set(latencyRequirement), Some(Set(reqChange)))

      case Mode.MADRID_TRACES =>
        this.runSimulation(1, PlacementStrategy.getStrategyByName(startingPlacementAlgorithm), transitionMode,
          () => log.info("MadridTrace simulation ended"),
          Set(latencyRequirement), Some(Set()))

      case Mode.LINEAR_ROAD =>
        this.runSimulation(1, PlacementStrategy.getStrategyByName(startingPlacementAlgorithm), transitionMode,
          () => log.info("LinearRoad simulation ended"),
          Set(latencyRequirement), Some(Set()))

      case Mode.YAHOO_STREAMING =>
        this.runSimulation(1, PlacementStrategy.getStrategyByName(startingPlacementAlgorithm), transitionMode,
          () => log.info("YahooStreaming simulation ended"),
          Set(latencyRequirement), Some(Set()))
    }

  } catch {
    case e: Throwable => log.error(e, "failed to run simulation")
  }

  def shutdown() = {
    import scala.sys.process._
    ("pkill -f ntpd").!
    CoordinatedShutdown.get(cluster.system).run(CoordinatedShutdown.ClusterDowningReason)
    this.cluster.system.terminate()

  }

}

case class RecordLatency() extends LatencyMeasurement {
  var lastMeasurement: Option[java.time.Duration] = Option.empty

  override def apply(latency: java.time.Duration): Any = {
    lastMeasurement = Some(latency)
  }
}

case class RecordMessageHops() extends MessageHopsMeasurement {
  var lastMeasurement: Option[Int] = Option.empty

  override def apply(overhead: Int): Any = {
    lastMeasurement = Some(overhead)
  }
}

case class RecordAverageLoad() extends LoadMeasurement {
  var lastLoadMeasurement: Option[Double] = Option.empty

  def apply(load: Double): Any = {
    lastLoadMeasurement = Some(load)
  }
}

case class RecordFrequency() extends FrequencyMeasurement {
  var lastMeasurement: Option[Int] = Option.empty

  override def apply(frequency: Int): Any = {
    lastMeasurement = Some(frequency)
  }
}

case class RecordTransitionStatus() extends TransitionMeasurement {
  var lastMeasurement: Option[Int] = Option.empty

  override def apply(status: Int): Any = {
    lastMeasurement = Some(status)
  }
}

case class RecordMessageOverhead() extends MessageOverheadMeasurement {
  var lastEventOverheadMeasurement: Option[Long] = Option.empty
  var lastPlacementOverheadMeasurement: Option[Long] = Option.empty

  override def apply(eventOverhead: Long, placementOverhead: Long): Any = {
    lastEventOverheadMeasurement = Some(eventOverhead)
    lastPlacementOverheadMeasurement = Some(placementOverhead)
  }
}

case class RecordNetworkUsage() extends NetworkUsageMeasurement {
  var lastUsageMeasurement: Option[Double] = Option.empty

  override def apply(status: Double): Any = {
    lastUsageMeasurement = Some(status)
  }
}

case class RecordPublishingRate() extends PublishingRateMeasurement {
  var lastRateMeasurement: Option[Double] = Option.empty

  override def apply(status: Double): Any = {
    lastRateMeasurement = Some(status)
  }
}


case class RecordProcessingNodes() {
  var lastMeasurement: Option[List[Address]] = None

  def apply(status: List[Address]): Any = {
    lastMeasurement = Some(status)
  }
}

case class AllRecords(recordLatency: RecordLatency = RecordLatency(),
                      recordAverageLoad: RecordAverageLoad = RecordAverageLoad(),
                      recordMessageHops: RecordMessageHops = RecordMessageHops(),
                      recordFrequency: RecordFrequency = RecordFrequency(),
                      recordMessageOverhead: RecordMessageOverhead = RecordMessageOverhead(),
                      recordNetworkUsage: RecordNetworkUsage = RecordNetworkUsage(),
                      recordPublishingRate: RecordPublishingRate = RecordPublishingRate(),
                      recordTransitionStatus: Option[RecordTransitionStatus] = Some(RecordTransitionStatus()),
                      recordProcessingNodes: Option[RecordProcessingNodes] = Some(RecordProcessingNodes())) {
  def allDefined: Boolean =
    recordLatency.lastMeasurement.isDefined &&
    recordMessageHops.lastMeasurement.isDefined &&
    recordAverageLoad.lastLoadMeasurement.isDefined &&
    recordFrequency.lastMeasurement.isDefined &&
    recordMessageOverhead.lastEventOverheadMeasurement.isDefined &&
    recordMessageOverhead.lastPlacementOverheadMeasurement.isDefined &&
    recordNetworkUsage.lastUsageMeasurement.isDefined /*&&
    recordPublishingRate.lastRateMeasurement.isDefined*/
  /*
  SpecialStats.log(this.getClass.getSimpleName, "measurements", s"latency:${recordLatency.lastMeasurement.isDefined}")
  SpecialStats.log(this.getClass.getSimpleName, "measurements", s"message hops:${recordMessageHops.lastMeasurement.isDefined}")
  SpecialStats.log(this.getClass.getSimpleName, "measurements", s"average load:${recordAverageLoad.lastLoadMeasurement.isDefined}")
  SpecialStats.log(this.getClass.getSimpleName, "measurements", s"frequency :${recordFrequency.lastMeasurement.isDefined}")
  SpecialStats.log(this.getClass.getSimpleName, "measurements", s"event overhead :${recordOverhead.lastEventOverheadMeasurement.isDefined}")
  SpecialStats.log(this.getClass.getSimpleName, "measurements", s"event overhead :${recordOverhead.lastPlacementOverheadMeasurement.isDefined}")
  SpecialStats.log(this.getClass.getSimpleName, "measurements", s"network usage :${recordNetworkUsage.lastUsageMeasurement.isDefined}")
  */
  def getRecordsList: List[Measurement] = List(recordLatency, recordAverageLoad, recordMessageHops, recordFrequency, recordMessageOverhead, recordNetworkUsage, recordPublishingRate, recordTransitionStatus.get)
  def getValues = this.getRecordsList.map {
    case l: RecordLatency => l -> l.lastMeasurement
    case l: RecordAverageLoad => l -> l.lastLoadMeasurement
    case h: RecordMessageHops => h -> h.lastMeasurement
    case f: RecordFrequency => f -> f.lastMeasurement
    case o: RecordMessageOverhead => o -> (o.lastEventOverheadMeasurement, o.lastPlacementOverheadMeasurement)
    case n: RecordNetworkUsage => n -> n.lastUsageMeasurement
    case p: RecordPublishingRate => p -> p.lastRateMeasurement
    case t: RecordTransitionStatus => t -> t.lastMeasurement
    case pn: RecordProcessingNodes => pn -> pn.lastMeasurement
  }
}

case class MobilityData(publisherSection: Int, speed: Double)
case object SetMissingBandwidthMeasurementDefaults
case class LinearRoadDataNew(vehicleId: Int, section: Int, density: Int, speed: Double, var change: Boolean = false, var dataWindow: Option[List[Double]] = None, var avgSpeed: Option[Double] = None)
case class YahooDataNew(adId: Int, eventType: Int, var campaignId: Option[Int] = None)
case class StatisticData(id: Int, performance: Double)