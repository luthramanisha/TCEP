package tcep.graph.nodes

import akka.actor.ActorRef
import tcep.data.Events._
import tcep.data.Queries.DatabaseJoinQuery
import tcep.factories.NodeFactory
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.nodes.traits.{TransitionConfig, UnaryNode}
import tcep.graph.{CreatedCallback, EventCallback}
import tcep.placement.HostInfo
import tcep.simulation.tcep.YahooDataNew

import scala.collection.mutable.ListBuffer

case class DatabaseJoinNode(
                             transitionConfig: TransitionConfig,
                             hostInfo: HostInfo,
                             backupMode: Boolean,
                             mainNode: Option[ActorRef],
                             query: DatabaseJoinQuery,
                             createdCallback: Option[CreatedCallback],
                             eventCallback: Option[EventCallback],
                             isRootOperator: Boolean,
                             _parentNode: ActorRef*
                           ) extends UnaryNode {

  var parentNode: ActorRef = _parentNode.head

  override def preStart(): Unit = {
    super.preStart()
  }


  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      val s = sender()
      if(parentsList.contains(s)) {
        val value: List[YahooDataNew] = event match {
          case Event1(e1) =>
            this.handle(List(e1))
          case Event2(e1, e2) =>
            this.handle(List(e1, e2))
          case Event3(e1, e2, e3) =>
            this.handle(List(e1, e2, e3))
          case Event4(e1, e2, e3, e4) =>
            this.handle(List(e1, e2, e3, e4))
          case Event5(e1, e2, e3, e4, e5) =>
            this.handle(List(e1, e2, e3, e4, e5))
          case Event6(e1, e2, e3, e4, e5, e6) =>
            this.handle(List(e1, e2, e3, e4, e5, e6))
        }
        for (data <- value) {
          val dbEvent = Event1(data)
          dbEvent.monitoringData = event.monitoringData
          emitEvent(dbEvent)
        }
      }
    case unhandledMessage => log.info(s"${self.path.name} received msg ${unhandledMessage.getClass} from ${sender()}")
  }

  def handle(dataList: List[Any]) = {
    var out = ListBuffer.empty[YahooDataNew]
    for (data <- dataList) {
      data match {
        case y: YahooDataNew =>
          if (this.query.db.contains(y.adId))
            y.campaignId = Some(this.query.db.get(y.adId).get)
          else {
            y.campaignId = Some(-1)
            //this.reloading = true
          }
          out += y
      }
    }
    out.toList
  }
/*
  def reload() = {
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
          if (!this.storage.contains(adId))
            this.storage += (adId -> campId)
        }
      }
      bufferedSource.close()
    } catch {
      case e: Throwable => log.error(s"error while creating database join Nodefrom tracefile $dbFile: {}", e)
    }
    log.info(s"DBJOIN storage: ${this.storage.keySet}")
    this.reloading = false
  }
*/
}
