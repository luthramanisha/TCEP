package tcep.machinenodes.consumers

import com.typesafe.config.ConfigFactory
import tcep.data.Queries
import tcep.data.Queries._
import tcep.dsl.Dsl._
import tcep.simulation.tcep.{LinearRoadDataNew, MobilityData}

class TollComputingConsumer extends Consumer {

  val windowSize: Int = 60*3

  override def preStart(): Unit = {
    super.preStart()
    log.info("Starting TollComputingConsumer with requirements: Latency < 5000ms!")
  }

  override def receive: Receive = super.receive

  override def queryFunction(): Queries.Query = {
    val streams = this.eventStreams(0).asInstanceOf[Vector[Stream1[LinearRoadDataNew]]]
    val requirement = latency < timespan(800.milliseconds) otherwise None
    val and01 = streams(0).and(streams(1))
    val and23 = streams(2).and(streams(3))
    val and45 = streams(4).and(streams(5))
    val and0123 = and01.and(and23)
    val and012345 = and0123.and(and45)
    val sectionAvgSpeeds = 50 to 52 map (section => { //One less than actual sections because last cant generate toll
      val window = SlidingWindow6(and012345, Set(), windowSize=5*60, sectionFilter = Some(section))
      NewAverage1(window)
    })
    val observer = ObserveChange6(and012345)
    val avg01 = sectionAvgSpeeds(0).and(sectionAvgSpeeds(1))
    val join = avg01.join(observer, slidingWindow(1.seconds), slidingWindow(1.seconds), requirement)
    join

    /* OLD QUERY
    val and01 = streams(0).and(streams(1))
    val and23 = streams(2).and(streams(3))
    val and45 = streams(4).and(streams(5))
    val and0123 = and01.and(and23)
    val and012345 = and0123.and(and45)
    val sectionAvgSpeeds = 47 to 52 map (section => { //One less than actual sections because last cant generate toll
      val window = SlidingWindow6(and012345, Set(), windowSize=5*60, sectionFilter = Some(section))
      NewAverage1(window)
    })
    val observer = ObserveChange6(and012345)
    val avg01 = sectionAvgSpeeds(0).and(sectionAvgSpeeds(1))
    val avg23 = sectionAvgSpeeds(2).and(sectionAvgSpeeds(3))
    val avg0123 = avg01.and(avg23)
    val avg01234 = avg0123.and(sectionAvgSpeeds(4))
    val join = avg01234.join(observer, slidingWindow(1.seconds), slidingWindow(1.seconds), requirement)
    join
     */
  }
}
