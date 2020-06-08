package tcep.graph.qos

import org.slf4j.LoggerFactory
import tcep.data.Events.Event
import tcep.data.Queries._
import tcep.dsl.Dsl.LoadMeasurement

case class LoadMonitor(query: Query, record: Option[LoadMeasurement]) extends Monitor{
  val log = LoggerFactory.getLogger(getClass)
  var loadRequirement: Option[LoadRequirement] = query.requirements.collect { case lr: LoadRequirement => lr }.headOption

  override def onEventEmit(event: Event, status: Int): Unit = {
    val currentLoad = event.monitoringData.averageLoad

    if(record.isDefined) record.get.apply(currentLoad)

    if(loadRequirement.isDefined && loadRequirement.get.otherwise.isDefined){
      val loadRequirementVal = loadRequirement.get.machineLoad.value
      loadRequirement.get.operator match {
        case Equal =>        if (!(currentLoad == loadRequirementVal)) loadRequirement.get.otherwise.get.apply(currentLoad)
        case NotEqual =>     if (!(currentLoad != loadRequirementVal)) loadRequirement.get.otherwise.get.apply(currentLoad)
        case Greater =>      if (!(currentLoad >  loadRequirementVal)) loadRequirement.get.otherwise.get.apply(currentLoad)
        case GreaterEqual => if (!(currentLoad >= loadRequirementVal)) loadRequirement.get.otherwise.get.apply(currentLoad)
        case Smaller =>      if (!(currentLoad <  loadRequirementVal)) loadRequirement.get.otherwise.get.apply(currentLoad)
        case SmallerEqual => if (!(currentLoad <= loadRequirementVal)) loadRequirement.get.otherwise.get.apply(currentLoad)
      }
    }
  }
}

case class LoadMonitorFactory(query: Query, record: Option[LoadMeasurement]) extends MonitorFactory {
  override def createNodeMonitor: Monitor = LoadMonitor(query, record)
}
