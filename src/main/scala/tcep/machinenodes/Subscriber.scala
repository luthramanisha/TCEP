package tcep.machinenodes

import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorRef, RootActorPath}
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.{Member, MemberStatus}
import akka.util.Timeout
import tcep.data.Events.{Event, Event1, Event3}
import tcep.data.Queries._
import tcep.data.Structures.MachineLoad
import tcep.dsl.Dsl._
import tcep.graph.nodes.traits.{TransitionConfig, TransitionModeNames}
import tcep.graph.qos._
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}
import tcep.placement.vivaldi.VivaldiCoordinates

import scala.concurrent.{Await, Future}


/**
  * Subscriber Actor
  */
class Subscriber extends VivaldiCoordinates with ActorLogging {

  var publishers: Map[String, ActorRef] = Map.empty[String, ActorRef]
  val actorSystem = this.context.system

  override def currentClusterState(state: CurrentClusterState): Unit = {
    super.currentClusterState(state)
    state.members.filter(x => x.status == MemberStatus.Up && x.hasRole("Publisher")) foreach extractProducers

    checkAndRunQuery()
  }

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

  def checkAndRunQuery(): Unit = {
    if (publishers.keySet == Set("A", "B", "C", "D")) {
      runQuery()
    }
  }


  def runQuery(): Unit = {
    log.info("All publishers are available now, executing query")

    //TODO: define callbacks
    val latencyRequirement = latency < timespan(1.milliseconds) otherwise Option.empty
    val loadRequirement = load < MachineLoad(0.5) otherwise Option.empty
    val messageOverheadRequirement = hops < 10 otherwise Option.empty

    val query1: Query3[Either[Int, String], Either[Int, X], Either[Float, X]] =
      stream[Int]("A")
        .join(
          stream[Int]("B"),
          slidingWindow(2.seconds),
          slidingWindow(5.seconds))
        .dropElem1(latencyRequirement)
        .selfJoin(
          tumblingWindow(1.seconds),
          tumblingWindow(10.seconds))
        .and(stream[Float]("C"))
        .or(stream[String]("D"))

    val monitors: Array[MonitorFactory] = Array(AverageFrequencyMonitorFactory(query1, Option.empty),
                                                DummyMonitorFactory(query1))

    val queryGraph = new QueryGraph(this.context, cluster, query1, TransitionConfig(), publishers, None, Some(GraphCreatedCallback()), monitors)

    queryGraph.createAndStart(null)(Some(EventPublishedCallback()))

    //change in requirements after 2 minutes
    val f = Future {
      Thread.sleep(2 * 60 * 1000)
    }

    f.onComplete(_ => {
      log.info("adding new demand! Time for transition")
      queryGraph.removeDemand(Seq(latencyRequirement))
      queryGraph.addDemand(Seq(messageOverheadRequirement))
    })
  }
}


//Using Named classes for callbacks instead of anonymous ones due to serialization issues
case class GraphCreatedCallback() extends CreatedCallback {
  override def apply(): Any = {
    println("STATUS:\t\tGraph has been created.")
  }
}

case class EventPublishedCallback() extends EventCallback {
  override def apply(event: Event): Any = event match {
    case Event3(i1, i2, f) => println(s"COMPLEX EVENT:\tEvent3($i1,$i2,$f)")
    case Event1(s) => println(s"COMPLEX EVENT:\tEvent1($s)")
    case _ =>
  }
}

object SubscriberMessages {
  case object SubmitQuery
}
