package tcep.simulation.adaptive.cep

import java.io.{File, PrintStream}
import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorRef, ActorSystem, RootActorPath}
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.{Member, MemberStatus}
import akka.util.Timeout
import tcep.data.Queries.Query
import tcep.dsl.Dsl._
import tcep.placement.vivaldi.VivaldiCoordinates
import tcep.system.System

import scala.concurrent.Await


class SimulationSetup(directory: Option[File]) extends VivaldiCoordinates with ActorLogging {

  implicit val actorSystem: ActorSystem = context.system
  var publishers: Map[String, ActorRef] = Map.empty[String, ActorRef]

  //used for finding Publisher nodes and Benchmark node
  override def currentClusterState(state: CurrentClusterState): Unit = {
    super.currentClusterState(state)
    state.members.filter(x => x.status == MemberStatus.Up && x.hasRole("Publisher")) foreach extractProducers

    if (state.members.exists(x => x.status == MemberStatus.Up && x.hasRole("Publisher"))) {
      log.info("found benchmark node")
      checkAndRunQuery()
    }
  }

  //used for finding Publisher nodes and Benchmark node
  override def memberUp(member: Member): Unit = {
    super.memberUp(member)
    if (member.hasRole("Publisher")) extractProducers(member)
    else log.info(s"found: ${member.roles}")
  }

  def extractProducers(member: Member): Unit = {
    log.info("Found publisher node")
    implicit val resolveTimeout = Timeout(60, TimeUnit.SECONDS)
    val actorRef = Await.result(context.actorSelection(RootActorPath(member.address) / "user" / "*").resolveOne(), resolveTimeout.duration)
    publishers += (actorRef.path.name -> actorRef)
    log.info(s"saving publisher ${actorRef.path.name}")

    checkAndRunQuery()
  }

  //If all publishers required in the query are available then run the simulation
  def checkAndRunQuery(): Unit = {
    if (Set("A", "B", "C").subsetOf(publishers.keySet)) {
      executeQueries()
    }else{
      log.info(s"not executing $publishers")
    }
  }

  val query0: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .dropElem1()

  val query1: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .join(
        stream[Int]("C")
          .where(0 <= _),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .dropElem1()

  val queries = Seq("query0" -> query0)


  def run(queries: Query*)(out: PrintStream)(optimize: (Simulation, Long, Long, Long) => Unit) = {
    val steps = 3000
    val outputSampleSeconds = 10

    def createSimulation = {
      val system = new System(this.context)
      queries foreach {
        system runQuery(_, publishers, None, None)
      }

      val simulation = new Simulation(system)
      simulation.placeSequentially()

      simulation
    }

    val simulationStatic = createSimulation
    val simulationAdaptive = createSimulation

    optimize(simulationAdaptive, 0, Int.MaxValue, 0)
    optimize(simulationAdaptive, 0, Int.MaxValue, 0)

    val res = new StringBuilder

     res ++= "time-s,latencystatic-ms,latencyadaptive-ms,bandwidthstatic,bandwidthadaptive\n"

    0 to steps foreach { step =>
      val time = simulationStatic.currentTime.toSeconds
      val simulationStaticLatency = simulationStatic.measureLatency.toMillis
      val simulationAdaptiveLatency = simulationAdaptive.measureLatency.toMillis
      val simulationStaticBandwidth = simulationStatic.measureBandwidth.toLong
      val simulationAdaptiveBandwidth = simulationAdaptive.measureBandwidth.toLong

      if ((time % outputSampleSeconds) == 0){
        res ++= Seq(time, simulationStaticLatency, simulationAdaptiveLatency, simulationStaticBandwidth, simulationAdaptiveBandwidth) mkString ","
        res += '\n'
      }

      optimize(simulationAdaptive, time, simulationAdaptiveLatency, simulationAdaptiveBandwidth)

      simulationStatic.advance()
      simulationAdaptive.advance()
    }

    log.info("Simulation Result \n")
    log.info(res.toString)

    out.append(res)
  }


  def executeQueries(): Unit = {

    println("Starting simulation")

    queries foreach { case (name, query) =>
      runSimulation(query)(directory, s"$name-latency-everymin") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
        if ((timeSeconds % 60) == 0 && latencyMillis > 80)
          simulation.placeOptimizingLatency()
      }

      runSimulation(query)(directory, s"$name-latency-everysec") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
        if (latencyMillis > 80)
          simulation.placeOptimizingLatency()
      }

      runSimulation(query)(directory, s"$name-bandwidth-everymin") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
        if ((timeSeconds % 60) == 0 && bandwidth < 25)
          simulation.placeOptimizingBandwidth()
      }

      runSimulation(query)(directory, s"$name-bandwidth-everysec") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
        if (bandwidth < 40)
          simulation.placeOptimizingBandwidth()
      }

      runSimulation(query)(directory, s"$name-latencybandwidth-everymin") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
        if ((timeSeconds % 60) == 0 && (latencyMillis > 80 || bandwidth < 25))
          simulation.placeOptimizingLatencyAndBandwidth()
      }

      runSimulation(query)(directory, s"$name-latencybandwidth-everysec") { (simulation, timeSeconds, latencyMillis, bandwidth) =>
        if (latencyMillis > 80 || bandwidth < 25)
          simulation.placeOptimizingLatencyAndBandwidth()
      }
    }

    println("Simulation finished")
  }


  def runSimulation(queries: Query*)(directory: Option[File], name: String)(optimize: (Simulation, Long, Long, Long) => Unit) = {
    println(s"Simulating $name")

    val out = directory map { directory => new PrintStream(new File(directory, s"$name.csv")) } getOrElse java.lang.System.out
    out.println(name)
    run(queries: _*)(out)(optimize)
    directory foreach { _ => out.close() }
  }

}
