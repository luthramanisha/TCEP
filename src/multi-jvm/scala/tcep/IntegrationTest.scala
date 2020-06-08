package tcep

import java.io.File

import akka.actor.{ActorRef, PoisonPill, Props}
import tcep.dsl.Dsl.{Seconds => _, TimespanHelper => _}
import tcep.graph.nodes.traits.TransitionConfig
import tcep.simulation.tcep.{Mode, SimulationSetup}

import scala.concurrent.duration._

// need one concrete test class per node
class IntegrationMultiJvmNode1 extends IntegrationMultiNodeTestSpec
class IntegrationMultiJvmNode2 extends IntegrationMultiNodeTestSpec
class IntegrationMultiJvmClient extends IntegrationMultiNodeTestSpec
class IntegrationMultiJvmPublisher1 extends IntegrationMultiNodeTestSpec
class IntegrationMultiJvmPublisher2 extends IntegrationMultiNodeTestSpec

abstract class IntegrationMultiNodeTestSpec extends MultiJVMTestSetup {

  import TCEPMultiNodeConfig._

  "Integration test MFGS transition" must {
    "successfully deploy the operator graph to the candidates and transit from MDCEP to Relaxation" in {
      testConductor.enter("test integration test start")

      runOn(client) {

        val dir = "logs"
        val directory = if (new File(dir).isDirectory) Some(new File(dir)) else {
          log.info("Invalid directory path")
          None
        }
        val publisherNames = Some(Vector("P:localhost:2501", "P:localhost:2502")) // need to override publisher names because all nodes have hostname (localhost) during test
        val simRef: ActorRef = system.actorOf(Props(new SimulationSetup(directory, 2, transitionMode = TransitionConfig(), durationInMinutes = Some(2), startingPlacementAlgorithm = "Relaxation", overridePublisherPorts = Some(Set(2501, 2502)))), "SimulationSetup")
        system.scheduler.scheduleOnce(90 seconds)(() => simRef ! PoisonPill)
        val start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start <= 90000) {}

      }
      val start = System.currentTimeMillis()
      while (System.currentTimeMillis() - start <= 150000) {}

      testConductor.enter("integration test end")
    }
  }
/*
  "Integration test SMS transition" must {
    "successfully deploy the operator graph to the candidates and transit from Relaxation to Starks" in {
      testConductor.enter("test integration test start")

      runOn(client) {

        val dir = "logs"
        val directory = if (new File(dir).isDirectory) Some(new File(dir)) else {
          log.info("Invalid directory path")
          None
        }
        val publisherNames = Some(Vector("P:localhost:2501", "P:localhost:2502")) // need to override publisher names because all nodes have hostname (localhost) during test
        val simRef: ActorRef = system.actorOf(Props(new SimulationSetup(directory, Mode.TEST_SMS, Some(2), Some("Relaxation"), publisherNames)), "SimulationSetup")
      }

      val start = System.currentTimeMillis()
      while (System.currentTimeMillis() - start <= 150000) {}
      testConductor.enter("integration test end")
    }
  }

  "Integration test SPLC data collection" must {
    "successfully deploy the operator graph to the candidates" in {
      testConductor.enter("test integration test start")

      runOn(client) {

        val dir = "logs"
        val directory = if (new File(dir).isDirectory) Some(new File(dir)) else {
          log.info("Invalid directory path")
          None
        }
        val publisherNames = Some(Vector("P:localhost:2501", "P:localhost:2502")) // need to override publisher names because all nodes have hostname (localhost) during test
        val simRef: ActorRef = system.actorOf(Props(new SimulationSetup(directory, Mode.SPLC_DATACOLLECTION, Some(2), Some("Rizou"), publisherNames)), "SimulationSetup")

      }

      val start = System.currentTimeMillis()
      while (System.currentTimeMillis() - start <= 150000) {}
      testConductor.enter("integration test end")
    }
  }
  */
}

