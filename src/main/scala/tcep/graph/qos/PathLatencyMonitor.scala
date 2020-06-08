package tcep.graph.qos

import java.time._

import org.slf4j.LoggerFactory
import tcep.data.Events.Event
import tcep.data.Queries._
import tcep.dsl.Dsl.LatencyMeasurement

case class PathLatencyMonitor(query: Query, recordLatency: Option[LatencyMeasurement]) extends Monitor {

  val log = LoggerFactory.getLogger(getClass)
  var latencyRequirement: Option[LatencyRequirement] = query.requirements.collect { case lr: LatencyRequirement => lr }.headOption

  def isRequirementNotMet(latency: Duration, lr: LatencyRequirement): Boolean = {
    val met: Boolean = lr.operator match {
      case Equal => latency.compareTo(lr.latency) == 0
      case NotEqual => latency.compareTo(lr.latency) != 0
      case Greater => latency.compareTo(lr.latency) > 0
      case GreaterEqual => latency.compareTo(lr.latency) >= 0
      case Smaller => latency.compareTo(lr.latency) < 0
      case SmallerEqual => latency.compareTo(lr.latency) <= 0
    }
    !met
  }

  override def onEventEmit(event: Event, status: Int): X = {
    /**
      * @author Niels
      * calculate path latency in two ways:
      * 1) time between emission of the first event involved in the query on a publisher and receiving the final event at the clientNode
      * 2) accumulated latencies between operators (step by step measurements) -> see which is more accurate
     */
    val accumulatedLatency = Duration.ofMillis(event.monitoringData.latency)
    val startTime: Long = event.monitoringData.creationTimestamp
    val end2endLatencyMeasurement = Duration.ofMillis(System.currentTimeMillis() - startTime)
    //SpecialStats.debug("PathLatencyMonitor", s"accumulatedLatency: ${accumulatedLatency}")
    //SpecialStats.debug("PathLatencyMonitor", s"end2endLatency: ${end2endLatencyMeasurement}")

    if (recordLatency.isDefined) recordLatency.get.apply(end2endLatencyMeasurement)
    if (latencyRequirement.isDefined && latencyRequirement.get.otherwise.isDefined &&
      isRequirementNotMet(end2endLatencyMeasurement, latencyRequirement.get)) {
      latencyRequirement.get.otherwise.get.apply(end2endLatencyMeasurement)
      log.debug("latency requirement: latency " +  latencyRequirement.get.operator + latencyRequirement.get.latency +
              " violated, current latency: " + end2endLatencyMeasurement + " otherwise: "+ latencyRequirement.get.otherwise.get)
    }
  }
}

case class PathLatencyMonitorFactory(query: Query, recordLatency: Option[LatencyMeasurement]) extends MonitorFactory {
  override def createNodeMonitor: Monitor = PathLatencyMonitor(query, recordLatency)
}
