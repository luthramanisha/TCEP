package tcep.machinenodes.consumers

import akka.actor.ActorLogging
import tcep.data.Queries._
import tcep.data.Structures.MachineLoad
import tcep.dsl.Dsl.{hops, latency, timespan, _}
import tcep.simulation.tcep.MobilityData


/**
* Subscriber Actor
*/
class AvgSpeedConsumer extends Consumer {


  override def preStart(): Unit = {
    super.preStart()
    log.info(s"Starting AverageSpeedConsumer with requirements: Latency < 500ms and Machineload < 10.0!")
  }

  override def receive: Receive = super.receive

  def queryFunction(): Average2[MobilityData] = {
    val streams = this.eventStreams(0).asInstanceOf[Vector[Stream1[MobilityData]]]
    val join01 = streams(0).and(streams(1))
    val join23 = streams(2).and(streams(3))
    val join0123 = join01.and(join23)
    val averageA = Average4(join0123)

    val join67 = streams(6).and(streams(7))
    val join89 = streams(8).and(streams(9))
    val join6789 = join67.and(join89)
    val averageB = Average4(join6789)

    val joinAB = averageA.and(averageB)
    val requirements = Set[Requirement](
      latency < timespan(500.milliseconds) otherwise None,
      load < MachineLoad(10.0d) otherwise None
    )
    val averageAB = Average2(joinAB, requirements)
    averageAB
  }
}
