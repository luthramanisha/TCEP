package tcep.graph.qos

import tcep.data.Events.Event
import tcep.data.Queries.Query

case class DummyMonitorFactory(query: Query) extends MonitorFactory {

  override def createNodeMonitor: Monitor = (event: Event, status: Int) => {
    //ignoring messages
  }
}

