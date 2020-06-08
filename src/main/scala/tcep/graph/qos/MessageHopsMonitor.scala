package tcep.graph.qos

import org.slf4j.LoggerFactory
import tcep.data.Events.Event
import tcep.data.Queries.{MessageHopsRequirement, _}
import tcep.dsl.Dsl.MessageHopsMeasurement
import tcep.simulation.tcep.RecordProcessingNodes

case class MessageHopsMonitor(query: Query, record: Option[MessageHopsMeasurement], recordProcessingNodes: Option[RecordProcessingNodes]) extends Monitor{

  var logger = LoggerFactory.getLogger(getClass)
  val hopsRequirement = query.requirements.collect { case lr: MessageHopsRequirement => lr }.headOption


  override def onEventEmit(event: Event, status: Int): Unit = {
    val hops = event.monitoringData.messageHops

    if(record.isDefined) record.get.apply(hops)

    if(hopsRequirement.isDefined && hopsRequirement.get.otherwise.isDefined){
      val hopsRequirementVal = hopsRequirement.get.requirement
      hopsRequirement.get.operator match {
        case Equal =>        if (!(hops == hopsRequirementVal)) hopsRequirement.get.otherwise.get.apply(hops)
        case NotEqual =>     if (!(hops != hopsRequirementVal)) hopsRequirement.get.otherwise.get.apply(hops)
        case Greater =>      if (!(hops >  hopsRequirementVal)) hopsRequirement.get.otherwise.get.apply(hops)
        case GreaterEqual => if (!(hops >= hopsRequirementVal)) hopsRequirement.get.otherwise.get.apply(hops)
        case Smaller =>      if (!(hops <  hopsRequirementVal)) hopsRequirement.get.otherwise.get.apply(hops)
        case SmallerEqual => if (!(hops <= hopsRequirementVal)) hopsRequirement.get.otherwise.get.apply(hops)
      }
    }

  }
}


case class MessageHopsMonitorFactory(query: Query, record: Option[MessageHopsMeasurement], recordProcessingNodes: Option[RecordProcessingNodes]) extends MonitorFactory {
  override def createNodeMonitor: Monitor = MessageHopsMonitor(query, record, recordProcessingNodes)
}
