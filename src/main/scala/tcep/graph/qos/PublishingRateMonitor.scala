package tcep.graph.qos

import org.slf4j.LoggerFactory
import tcep.data.Events.Event
import tcep.data.Queries.Query
import tcep.dsl.Dsl.PublishingRateMeasurement

case class PublishingRateMonitor(recordPublishingRate: PublishingRateMeasurement) extends Monitor {
  val log = LoggerFactory.getLogger(getClass)

  override def onEventEmit(event: Event, transitionStatus: Int): Unit = {

      recordPublishingRate(event.monitoringData.publishingRate)
    }
}

case class PublishingRateMonitorFactory(query: Query, publishingRateMeasurement: PublishingRateMeasurement) extends MonitorFactory {
  override def createNodeMonitor: Monitor = PublishingRateMonitor(publishingRateMeasurement)
}


