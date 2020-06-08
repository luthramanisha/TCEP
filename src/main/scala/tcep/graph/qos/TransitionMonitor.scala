package tcep.graph.qos

import org.slf4j.LoggerFactory
import tcep.data.Events.Event
import tcep.data.Queries._
import tcep.dsl.Dsl.TransitionMeasurement

case class TransitionMonitor(recordTransitionStatus: Option[TransitionMeasurement]) extends Monitor{
  val log = LoggerFactory.getLogger(getClass)

  override def onEventEmit(event: Event, transitionStatus: Int): X = {
    if(recordTransitionStatus.isDefined){
      recordTransitionStatus.get(transitionStatus)
    }
  }
}

case class TransitionMonitorFactory(query : Query, recordLatency: Option[TransitionMeasurement]) extends MonitorFactory {
  override def createNodeMonitor: Monitor = TransitionMonitor(recordLatency)
}
