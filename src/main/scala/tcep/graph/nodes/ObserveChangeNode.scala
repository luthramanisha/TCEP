package tcep.graph.nodes

import akka.actor.ActorRef
import tcep.data.Events._
import tcep.data.Queries.ObserveChangeQuery
import tcep.factories.NodeFactory
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.nodes.traits.{TransitionConfig, UnaryNode}
import tcep.graph.{CreatedCallback, EventCallback}
import tcep.placement.HostInfo
import tcep.simulation.tcep.LinearRoadDataNew

import scala.collection.mutable.{HashMap, ListBuffer}




case class ObserveChangeNode(
                              transitionConfig: TransitionConfig,
                              hostInfo: HostInfo,
                              backupMode: Boolean,
                              mainNode: Option[ActorRef],
                              query: ObserveChangeQuery,
                              createdCallback: Option[CreatedCallback],
                              eventCallback: Option[EventCallback],
                              isRootOperator: Boolean,_parentNode: ActorRef*
                            ) extends UnaryNode {

  var parentNode: ActorRef = _parentNode.head
  var storage: HashMap[Int, AnyVal] = HashMap.empty[Int, AnyVal]
  var lastEmit: Double = System.currentTimeMillis().toDouble

  override def preStart(): Unit = {
    super.preStart()
  }

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      val s = sender()
      if (parentsList.contains(s)) {
        val value: List[LinearRoadDataNew] = event match {
          case Event1(e1) =>
            this.getToForward(List(e1))
          case Event2(e1, e2) =>
            this.getToForward(List(e1, e2))
          case Event3(e1, e2, e3) =>
            this.getToForward(List(e1, e2, e3))
          case Event4(e1, e2, e3, e4) =>
            this.getToForward(List(e1, e2, e3, e4))
          case Event5(e1, e2, e3, e4, e5) =>
            this.getToForward(List(e1, e2, e3, e4, e5))
          case Event6(e1, e2, e3, e4, e5, e6) =>
            this.getToForward(List(e1, e2, e3, e4, e5, e6))
        }
        /*var changedData = ListBuffer.empty[(Int, Int)]
        for (data <- value) {
          if (data.change)
            changedData += ((data.vehicleId, data.section))
        }*/
        //if (value.size < 6) {
        for (data <- value) {
          val changeEvent = Event1(value.head)
          //Events.initializeMonitoringData(log, changeEvent, 1000.0d / (System.currentTimeMillis().toDouble-lastEmit), cluster.selfAddress)
          changeEvent.monitoringData = event.monitoringData
          lastEmit = System.currentTimeMillis().toDouble
          emitEvent(changeEvent)
        }
        //} else {
          /*var changedData = ListBuffer.empty[(Int, Int)]
          for (data <- value) {
            if (data.change)
              changedData += ((data.vehicleId, data.section))
          }
          val changeEvent = Event1(changedData.toList)
          Events.initializeMonitoringData(log, changeEvent, 1000.0d / (System.currentTimeMillis().toDouble-lastEmit), cluster.selfAddress)
          lastEmit = System.currentTimeMillis().toDouble
          emitEvent(changeEvent)
        }*/
      }
    case unhandledMessage => log.info(s"${self.path.name} received msg ${unhandledMessage.getClass} from ${sender()}")
  }

  def getToForward(dataList: List[Any]): List[LinearRoadDataNew] = {
    val events: ListBuffer[LinearRoadDataNew] = ListBuffer.empty[LinearRoadDataNew]
    for (value <- dataList) {
      value match {
        case l: LinearRoadDataNew =>
          if (this.storage.contains(l.vehicleId)) {
            val old = this.storage.get(l.vehicleId).get.asInstanceOf[Int]
            if (old < l.section) {
              this.storage += (l.vehicleId -> l.section)
              l.change = true
              events += l
            } else {
              events += l
            }
          } else {
            this.storage += (l.vehicleId -> l.section)
            l.change = true
            events += l
          }
        case _ =>
        }
      }
    events.toList
  }
}
