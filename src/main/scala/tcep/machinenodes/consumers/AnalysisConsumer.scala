package tcep.machinenodes.consumers
import tcep.data.Queries
import tcep.data.Queries.Stream1
import tcep.data.Queries._
import tcep.data.Structures.MachineLoad
import tcep.dsl.Dsl._
import tcep.simulation.tcep.YahooDataNew

import scala.collection.mutable.HashMap
import scala.io.Source

class AnalysisConsumer extends Consumer {

  var storage : HashMap[Int, Int] = HashMap.empty[Int, Int]

  override def preStart(): Unit = {
    super.preStart()
    val dbFile = s"/app/mobility_traces/yahooJoins.csv"
    try {
      val bufferedSource = Source.fromFile(dbFile)
      var headerSkipped = false
      for (line <- bufferedSource.getLines()){
        if (!headerSkipped)
          headerSkipped = true
        else {
          val cols = line.split(",").map(_.trim)
          val adId = cols(0).toInt
          val campId = cols(1).toInt
          this.storage += (adId -> campId)
        }
      }
      bufferedSource.close()
    } catch {
      case e: Throwable => log.error(s"error while creating database in consumer from file: $dbFile: {}", e)
    }
  }

  override def receive: Receive = super.receive

  override def queryFunction(): Queries.Query = {
    val streams = this.eventStreams(0).asInstanceOf[Vector[Stream1[YahooDataNew]]]
    val latencyRequirement = latency < timespan(700.milliseconds) otherwise None
    val loadRequirement = load < MachineLoad(3.0d) otherwise None
    val and01 = streams(0).and(streams(1))
    val and23 = streams(2).and(streams(3))
    val and45 = streams(4).and(streams(5))
    val and67 = streams(6).and(streams(7))
    val and0123 = and01.and(and23)
    val and4567 = and45.and(and67)
    val db0123 = DatabaseJoin4(and0123, Set(), this.storage)
    val db4567 = DatabaseJoin4(and4567, Set(), this.storage)
    val dbAnd01234567 = db0123.and(db4567)
    val purchaseFilter = ShrinkFilter2(dbAnd01234567, Set(), (event: Any) => {
      event.asInstanceOf[YahooDataNew].eventType == 2
    }, emitAlways = Some(false))
    val stats = WindowStatistic1(purchaseFilter, Set(latencyRequirement, loadRequirement), 60)
    //val stats = WindowStatistic1(purchaseFilter, Set(latencyRequirement), 60)
    stats
  }
}
