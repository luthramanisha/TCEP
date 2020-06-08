
package tcep.graph.transition.mapek.requirementBased

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import tcep.data.Queries
import tcep.data.Queries.{Requirement, Stream1}
import tcep.data.Structures.MachineLoad
import tcep.dsl.Dsl._
import tcep.graph.nodes.traits.{TransitionConfig, TransitionModeNames}
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.transition.MAPEK.{AddRequirement, RemoveRequirement, SetClient}
import tcep.graph.transition.{TransitionRequest, TransitionStats}
import tcep.placement.GlobalOptimalBDPAlgorithm
import tcep.placement.benchmarking.BenchmarkingNode
import tcep.placement.manets.StarksAlgorithm
import tcep.placement.sbon.PietzuchAlgorithm

import scala.collection.mutable.ListBuffer

class RequirementBasedMAPEKTest extends TestKit(ActorSystem())  with WordSpecLike with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
  }

  val latencyRequirement = latency < timespan(500.milliseconds) otherwise None
  val messageHopsRequirement = hops < 3 otherwise None
  val loadRequirement = load < MachineLoad(1.0) otherwise None

  class WrapperActor(transitionConfig: TransitionConfig, initialRequirements: Set[Requirement]) extends Actor {

    var mapek: RequirementBasedMAPEK = _
    var subscribers = ListBuffer[ActorRef]()
    override def preStart(): Unit = {
      super.preStart()
      val query = Stream1[Int]("A", initialRequirements)
      mapek = new RequirementBasedMAPEK(context, query, transitionConfig, BenchmarkingNode.selectBestPlacementAlgorithm(List(), Queries.pullRequirements(query, List()).toList))
      mapek.knowledge ! SetClient(testActor)
    }

    override def receive: Receive = {

      case message => mapek.monitor.forward(message)
    }
  }

  "RequirementBasedMAPEK" must {
    "propagate msgHops requirement addition, return Starks algorithm" in {
      val w = system.actorOf(Props( new WrapperActor(TransitionConfig(), Set(latencyRequirement))))

      Thread.sleep(3000)
      w ! RemoveRequirement(Seq(latencyRequirement))
      w ! AddRequirement(Seq(messageHopsRequirement, loadRequirement))
      expectMsg(TransitionRequest(StarksAlgorithm, w, TransitionStats()))

    }
  }

  "RequirementBasedMAPEK" must {
    "select GlobalOptimalBDP after latency requirement is removed and latency + msgHops requirement are added (Relaxation->GlobalOptimalBDP)" in {
      val w = system.actorOf(Props( new WrapperActor(TransitionConfig(), Set(latencyRequirement))))
      Thread.sleep(3000)
      w ! RemoveRequirement(Seq(latencyRequirement))
      w ! AddRequirement(Seq(latencyRequirement, messageHopsRequirement))
      expectMsg(TransitionRequest(GlobalOptimalBDPAlgorithm, w, TransitionStats()))
    }
  }


  "RequirementBasedMAPEK" must {
    "select Relaxation after latency and msgHops requirement is removed and latency requirement is added (GlobalOptimalBDP->Relaxation)" in {
      val w = system.actorOf(Props( new WrapperActor(TransitionConfig(), Set(latencyRequirement, messageHopsRequirement))))
      Thread.sleep(3000)
      w ! RemoveRequirement(Seq(latencyRequirement, messageHopsRequirement))
      w ! AddRequirement(Seq(latencyRequirement, loadRequirement))
      expectMsg(TransitionRequest(PietzuchAlgorithm, w, TransitionStats()))
    }
  }
}
