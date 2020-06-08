package tcep.graph.qos

import org.slf4j.LoggerFactory
import tcep.data.Events.Event
import tcep.data.Queries._
import tcep.dsl.Dsl.MessageOverheadMeasurement

case class MessageOverheadMonitor(recordOverhead: MessageOverheadMeasurement) extends Monitor {
  val log = LoggerFactory.getLogger(getClass)

  override def onEventEmit(event: Event, transitionStatus: Int): Unit = {
      recordOverhead(event.monitoringData.eventOverhead, event.monitoringData.placementOverhead)
  }
}

case class MessageOverheadMonitorFactory(query: Query, overheadMeasurement: MessageOverheadMeasurement) extends MonitorFactory {
  override def createNodeMonitor: Monitor = MessageOverheadMonitor(overheadMeasurement)
}
