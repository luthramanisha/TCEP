package tcep.simulation.adaptive.cep

import akka.actor.{Actor, ActorRef, Address, Props}
import akka.cluster.Cluster
import tcep.data.Events._
import tcep.data.Queries._
import tcep.data.Structures.MachineLoad
import tcep.dsl.Dsl._
import tcep.graph.QueryGraph
import tcep.graph.nodes.traits.{TransitionConfig, TransitionModeNames}
import tcep.graph.qos._
import tcep.publishers._

case class BaseActor() extends Actor {

  implicit val creatorAddress: Address = cluster.selfAddress
  val publisherA: ActorRef = this.context.actorOf(Props(RandomPublisher(id => Event1(id))),              "A")
  val publisherB: ActorRef = this.context.actorOf(Props(RandomPublisher(id => Event1(id * 2))),          "B")
  val publisherC: ActorRef = this.context.actorOf(Props(RandomPublisher(id => Event1(id.toFloat))),      "C")
  val publisherD: ActorRef = this.context.actorOf(Props(RandomPublisher(id => Event1(s"String($id)"))),  "D")

  val publishers: Map[String, ActorRef] = Map(
                                              "A" -> publisherA,
                                              "B" -> publisherB,
                                              "C" -> publisherC,
                                              "D" -> publisherD)

  case class RecordFrequency() extends FrequencyMeasurement{
    def apply(frequency: Int):Any = {
      println(s"node emits too few events $frequency")
    }

  }
  val query1: Query3[Either[Int, String], Either[Int, X], Either[Float, X]] =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(2.seconds),
        slidingWindow(2.seconds))
      .dropElem1(
        latency < timespan(1.milliseconds) otherwise Option.empty)
      .selfJoin(
        tumblingWindow(1.seconds),
        tumblingWindow(1.seconds),
        frequency > Frequency(3, 5) otherwise[RecordFrequency] Some(RecordFrequency()),
        frequency < Frequency(12, 15) otherwise Some(RecordFrequency()))
      .and(stream[Float]("C"))
      .or(stream[String]("D"))

  val query2: Query4[Int, Int, Float, String] =
    stream[Int]("A")
      .and(stream[Int]("B"))
      .join(
        sequence(
          nStream[Float]("C") -> nStream[String]("D"),
          frequency > Frequency(1, 5) otherwise Some(RecordFrequency())),
        slidingWindow(3.seconds),
        slidingWindow(3.seconds),
        hops < 10 otherwise Option.empty,
        load < MachineLoad.make(0.4) otherwise Option.empty,
        latency < timespan(20.seconds) otherwise Option.empty)


  val monitors: Array[MonitorFactory] = Array(AverageFrequencyMonitorFactory(query1, Option.empty),
                                              DummyMonitorFactory(query1))

  val cluster = Cluster(this.context.system)
  val graphFactory = new QueryGraph(this.context, Cluster(this.context.system), query1, TransitionConfig(), publishers, None, None, monitors)

  graphFactory.createAndStart(null)(
    eventCallback = Some({
      // Callback for `query1`:
      case Event3(i1, i2, f) => println(s"COMPLEX EVENT:\tEvent3($i1,$i2,$f)")
      case Event1(s) => println(s"COMPLEX EVENT:\tEvent1($s)")
      // Callback for `query2`:
      //case (i1, i2, f, s)             => println(s"COMPLEX EVENT:\tEvent4($i1, $i2, $f,$s)")
      // This is necessary to avoid warnings about non-exhaustive `match`:
      case _ =>
    }))

  override def receive: Receive = {
    case _ => println("not expecting any incomming messages")
  }
}
