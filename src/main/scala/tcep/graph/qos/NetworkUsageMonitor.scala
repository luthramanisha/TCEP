package tcep.graph.qos

import org.slf4j.LoggerFactory
import tcep.data.Events.Event
import tcep.data.Queries._
import tcep.dsl.Dsl.NetworkUsageMeasurement

case class NetworkUsageMonitor(recordNetworkUsage: NetworkUsageMeasurement) extends Monitor {
  val log = LoggerFactory.getLogger(getClass)

  override def onEventEmit(event: Event, transitionStatus: Int): Unit = {
      val usage = event.monitoringData.networkUsage
      recordNetworkUsage(usage.head)
  }
}

case class NetworkUsageMonitorFactory(query: Query, recordNetworkUsage: NetworkUsageMeasurement) extends MonitorFactory {
  override def createNodeMonitor: Monitor = NetworkUsageMonitor(recordNetworkUsage)
}
