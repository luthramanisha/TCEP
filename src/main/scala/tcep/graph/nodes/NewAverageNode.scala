package tcep.graph.nodes

import akka.actor.ActorRef
import tcep.data.Events.{Event, Event1}
import tcep.data.Queries.NewAverageQuery
import tcep.factories.NodeFactory
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.nodes.traits.{TransitionConfig, UnaryNode}
import tcep.graph.{CreatedCallback, EventCallback}
import tcep.placement.HostInfo
import tcep.simulation.tcep.LinearRoadDataNew

import scala.collection.mutable.ListBuffer

case class NewAverageNode(
                           transitionConfig: TransitionConfig,
                           hostInfo: HostInfo,
                           backupMode: Boolean,
                           mainNode: Option[ActorRef],
                           query: NewAverageQuery,
                           createdCallback: Option[CreatedCallback],
                           eventCallback: Option[EventCallback],
                           isRootOperator: Boolean,_parentNode: ActorRef*
                         ) extends UnaryNode {

  var parentNode: ActorRef = _parentNode.head
  var lastEmit = System.currentTimeMillis()

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      val s = sender()
      if (this.parentsList.contains(s)) {
        val avgData = event match {
          case Event1(e1) =>
            this.computeAverage(List(e1))
          case strange =>
            log.info(s"Strange Event received: $strange")
            List()
        }
        if (avgData.size > 0) {
          for (data <- avgData) {
            val avgEvent = Event1(data)
            avgEvent.monitoringData = event.monitoringData
            emitEvent(avgEvent)
          }
        } else {
          val emptyEvent = Event1(LinearRoadDataNew(-1, -1, -1, -100, false))
          emptyEvent.monitoringData = event.monitoringData
          emitEvent(emptyEvent)
        }
      }
    case unhandledMessage => log.info(s"${self.path.name} received msg ${unhandledMessage.getClass} from ${sender()}")
  }

  def computeAverage(dataList: List[Any]): List[LinearRoadDataNew] = {
    var out: ListBuffer[LinearRoadDataNew] = ListBuffer.empty[LinearRoadDataNew]
    for (data <- dataList) {
      data match {
        case l: LinearRoadDataNew =>
          var avg = 0.0
          for (speed <- l.dataWindow.get)
            avg += speed
          if (l.dataWindow.get.size > 0)
            avg = avg / l.dataWindow.get.size
          else
            avg = 0.0
          l.avgSpeed = Some(avg)
          out += l
      }
    }
    out.toList
  }
}
