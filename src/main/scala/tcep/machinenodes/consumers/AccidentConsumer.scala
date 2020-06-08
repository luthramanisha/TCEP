package tcep.machinenodes.consumers
import tcep.data.Queries
import tcep.data.Queries.{Stream1, _}
import tcep.dsl.Dsl._
import tcep.simulation.tcep.MobilityData


class AccidentConsumer extends Consumer {

  override def preStart(): Unit = {
    super.preStart()
    log.info(s"Starting AccidentConsumer with requirements: Latency < 1000ms!")
  }

  override def receive: Receive = super.receive

  // accident detection query: low speed, high density in section A, high speed, low density in following road section B
  override def queryFunction(): Queries.Query = {
    val sections = 0 to 1 map(section => { // careful when increasing number of subtrees (21 operators per tree) -> too many operators make transitions with MDCEP unreliable
      val avgSpeed = averageSpeedPerSectionQuery(section)
      //val density = this.eventStreams(1).asInstanceOf[Vector[Stream1[Int]]](section)
      //val join = avgSpeed.join(density, slidingWindow(1.seconds), slidingWindow(1.seconds))
      //join
      avgSpeed
      /*val filter = join.where(
        (_: MobilityData, _: Int) => true /*(sA: Double, dA: Double) => sA <= 60 && dA >= 10*/ // replace with always true to keep events (and measurements) coming in
        //(_: Any, _: Any) => true /*(sA: Double, dA: Double) => sA <= 60 && dA >= 10*/ // replace with always true to keep events (and measurements) coming in
      )
      filter*/
    })
    val requirement = latency < timespan(1000.milliseconds) otherwise None
    val and01 = sections(0).and(sections(1), requirement)
    and01
  }

  // average speed per section (average operator uses only values matching section)
  def averageSpeedPerSectionQuery(section: Int) = {

    val streams = this.eventStreams(0).asInstanceOf[Vector[Stream1[MobilityData]]]
    val and01 = streams(0).and(streams(1))
    val and23 = streams(2).and(streams(3))
    val and0123 = and01.and(and23)
    val averageA = Average4(and0123, sectionFilter = Some(section))

    val and67 = streams(4).and(streams(5))
    val and89 = streams(6).and(streams(7))
    val and6789 = and67.and(and89)
    val averageB = Average4(and6789, sectionFilter = Some(section))

    val andAB = averageA.and(averageB)
    val averageAB = Average2(andAB, sectionFilter = Some(section))
    averageAB
  }

  val testreq = latency < timespan(1000.milliseconds) otherwise None

  def debugQuery(parent: Query1[MobilityData] = this.eventStreams(0).asInstanceOf[Vector[Stream1[MobilityData]]].head, depth: Int = 20, count: Int = 0): Query1[MobilityData] = {
    if(count < depth) debugQuery(parent.where(_ => true), depth, count + 1)
    else parent.where(_ => true, testreq)
    val speedStreams = this.eventStreams(0).asInstanceOf[Vector[Stream1[MobilityData]]]
    Average2(speedStreams(0).and(speedStreams(1)), Set(testreq))

  }
  def debugQueryTree(depth: Int = 3, count: Int = 0): Average2[MobilityData] = {
    if(count < depth) Average2(debugQueryTree(depth, count + 1).and(debugQueryTree(depth, count + 1)))
    else averageSpeedPerSectionQuery(1) // 18 operators
  }

}
