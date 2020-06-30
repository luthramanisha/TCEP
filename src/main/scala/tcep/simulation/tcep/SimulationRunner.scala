package tcep.simulation.tcep

import java.io.File

import akka.actor.{ActorSystem, Props}
import org.discovery.vivaldi.DistVivaldiActor
import tcep.config.ConfigurationParser
import tcep.graph.nodes.traits.{TransitionConfig, TransitionExecutionModes, TransitionModeNames}
import tcep.machinenodes.helper.actors.TaskManagerActor
import tcep.placement.sbon.PietzuchAlgorithm
import tcep.utils.TransitionLogSubscriber

import scala.concurrent.ExecutionContextExecutor

//import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.sys.process._

/**
  * Runs the Transitive CEP simulation.
  * The code requires an optional commandline parameter for "directory path" where simulation results will be saved as
  * CSV files.
  * see docker_stack.yml file for more details.
  */
object SimulationRunner extends App with ConfigurationParser {

  logger.info("booting up simulation runner, args: " + args.toList.toString())

  if(args.length <3 ){
    logger.info("Not enough arguments")
  } else {
    executeSimulation()
  }

  def executeSimulation(): Unit = {
    try {
      logger.info(s"configuring simulation with options ${options.map(o => o._1 -> o._2.getClass)}")
      val dir = options('dir)
      val mode = options('mode)
      val duration = options.getOrElse('duration, "2")
      val initialAlgorithm = options.getOrElse('initialAlgorithm, PietzuchAlgorithm.name)
      val baseLatency = options.getOrElse('baseLatency, "0")
      val maxPubToClientHops = options.getOrElse('maxPubToClientHops, "0")
      val query = options.getOrElse('query, "Stream")
      val mapek = options.getOrElse('mapek, "requirementBased")
      val requirement = options.getOrElse('req, "latency")
      val eventRate = options.getOrElse('eventRate, "1")
      val transitionStrategy = options.getOrElse('transitionStrategy, "MFGS") match {
        case "MFGS" => TransitionModeNames.MFGS
        case "SMS" => TransitionModeNames.SMS
      }
      val transitionExecutionMode = options.getOrElse('transitionExecutionMode, "1").toInt match {
        case 0 => TransitionExecutionModes.SEQUENTIAL_MODE
        case 1 => TransitionExecutionModes.CONCURRENT_MODE
      }
      val loadTestMax = options.getOrElse('loadTestMax, "1").toInt
      logger.info(s"parsed options $options \n $transitionStrategy $transitionExecutionMode")

      val actorSystem: ActorSystem = ActorSystem(config.getString("clustering.cluster.name"), config)
      logger.info("created actorSystem")
      DistVivaldiActor.createVivIfNotExists(actorSystem)
      logger.info("created distViv")
      implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher
      Future {
        "ntpd -s" !
      }
      logger.info("ntpd started")

      val directory = if (new File(dir).isDirectory) Some(new File(dir)) else {
        logger.info("Invalid directory path")
        None
      }
      val fixedSimulationProperties = Map('baseLatency -> baseLatency.toInt, 'maxPubToClientHops -> maxPubToClientHops.toInt)
      val taskManagerActorProps = Props(classOf[TaskManagerActor]).withMailbox("prio-mailbox")
      val simulatorActorProps = Props(new SimulationSetup(directory, mode.toInt, TransitionConfig(transitionStrategy, transitionExecutionMode), Some(duration.toInt), initialAlgorithm, None, query, eventRate, fixedSimulationProperties, mapek, requirement, loadTestMax)).withMailbox("prio-mailbox")
      logger.info(s"taskManager mailbox: ${taskManagerActorProps.mailbox} \n simulator mailbox: ${simulatorActorProps.mailbox}")
      actorSystem.actorOf(taskManagerActorProps, "TaskManager")
      actorSystem.actorOf(simulatorActorProps, "SimulationSetup")
      actorSystem.actorOf(Props[TransitionLogSubscriber], "TransitionLogSubscriber")
    }
    catch {
      case e: Throwable => logger.error("failed to start actorsystem with subscriber", e)
    }
  }

  override def getArgs = args

  override def getRole = "Subscriber, Candidate"
}

object Mode {
  val TEST_RELAXATION = 1
  val TEST_STARKS = 2
  val TEST_SMS = 3
  val TEST_MFGS = 4
  val SPLC_DATACOLLECTION = 5
  val DO_NOTHING = 6
  val TEST_GUI = 7
  val TEST_RIZOU = 8
  val TEST_PC = 9
  val TEST_OPTIMAL = 10
  val TEST_RANDOM = 11
  val TEST_LIGHTWEIGHT = 12
  val MADRID_TRACES = 13
  val LINEAR_ROAD = 14
  val YAHOO_STREAMING = 15
}
