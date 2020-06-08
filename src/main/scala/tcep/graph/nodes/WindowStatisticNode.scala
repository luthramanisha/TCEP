package tcep.graph.nodes

import akka.actor.ActorRef
import tcep.data.Events._
import tcep.data.Queries.WindowStatisticQuery
import tcep.factories.NodeFactory
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.nodes.traits.{TransitionConfig, UnaryNode}
import tcep.graph.{CreatedCallback, EventCallback}
import tcep.placement.HostInfo
import tcep.simulation.tcep.{StatisticData, YahooDataNew}

import scala.collection.mutable.{HashMap, ListBuffer}

case class WindowStatisticNode(
                                transitionConfig: TransitionConfig,
                                hostInfo: HostInfo,
                                backupMode: Boolean,
                                mainNode: Option[ActorRef],
                                query: WindowStatisticQuery,
                                createdCallback: Option[CreatedCallback],
                                eventCallback: Option[EventCallback],
                                isRootOperator: Boolean,_parentNode: ActorRef*
                              ) extends UnaryNode {

  var parentNode: ActorRef = _parentNode.head
  var storage: HashMap[Int, ListBuffer[Double]] = HashMap.empty[Int, ListBuffer[Double]]

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      val updated = event match {
        case Event1(e1) =>
          this.store(List(e1))
        case Event2(e1, e2) =>
          this.store(List(e1, e2))
        case Event3(e1, e2, e3) =>
          this.store(List(e1, e2, e3))
        case Event4(e1, e2, e3, e4) =>
          this.store(List(e1, e2, e3, e4))
        case Event5(e1, e2, e3, e4, e5) =>
          this.store(List(e1, e2, e3, e4, e5))
        case Event6(e1, e2, e3, e4, e5, e6) =>
          this.store(List(e1, e2, e3, e4, e5, e6))
      }
      for (id <- updated) {
        val statEvent = Event1(StatisticData(id, this.storage.get(id).get.size))
        statEvent.monitoringData = event.monitoringData
        emitEvent(statEvent)
      }
    case unhandledMessage => log.info(s"${self.path.name} received msg ${unhandledMessage.getClass} from ${sender()}")
  }

  def store(dataList: List[Any]) = {
    val currentTime = System.currentTimeMillis() / 1000.0d //seconds
    val out = ListBuffer.empty[Int]
    for (data <- dataList) {
      data match {
        case y: YahooDataNew =>
          if (this.storage.contains(y.campaignId.get))
            this.storage.get(y.campaignId.get).get += currentTime
          else {
            this.storage += (y.campaignId.get -> ListBuffer.empty[Double])
            this.storage.get(y.campaignId.get).get += currentTime
          }
          val threshold = currentTime - this.query.windowSize
          while (this.storage.get(y.campaignId.get).get.size > 0 & this.storage.get(y.campaignId.get).get.head < threshold)
            this.storage += (y.campaignId.get -> this.storage.get(y.campaignId.get).get.tail)
          out += y.campaignId.get
      }
    }
    out.toList
  }

}
