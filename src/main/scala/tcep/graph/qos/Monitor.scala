package tcep.graph.qos

import tcep.data.Events._
import tcep.data.Queries._

trait Monitor {
  def onEventEmit(event: Event, transitionStatus: Int): Unit
}

trait MonitorFactory {
  val query: Query
  def createNodeMonitor: Monitor
}
