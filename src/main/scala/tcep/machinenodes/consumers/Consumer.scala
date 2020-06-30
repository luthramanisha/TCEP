package tcep.machinenodes.consumers

import akka.actor.ActorLogging
import tcep.data.Events.Event
import tcep.data.Queries.{Query, Stream1}
import tcep.graph.qos.{AverageFrequencyMonitorFactory, LoadMonitorFactory, MessageHopsMonitorFactory, Monitor, MonitorFactory, NetworkUsageMonitorFactory, PathLatencyMonitorFactory, MessageOverheadMonitorFactory, PublishingRateMonitorFactory, TransitionMonitorFactory}
import tcep.machinenodes.consumers.Consumer.{GetAllRecords, GetMonitorFactories, GetQuery, GetRequirementChange, SetQosMonitors, SetStatus, SetStreams}
import tcep.placement.vivaldi.VivaldiCoordinates
import tcep.simulation.tcep.{AllRecords, MobilityData}

abstract class Consumer extends VivaldiCoordinates with ActorLogging {

  lazy val query: Query = queryFunction()
  var allRecords: AllRecords = AllRecords()
  var monitorFactories: Array[MonitorFactory] = Array.empty[MonitorFactory]
  var monitors: Array[Monitor] = Array.empty[Monitor]
  var transitionStatus: Int = 0
  var eventStreams: Seq[Vector[Stream1[Any]]] = _

  override def postStop(): Unit = {
    log.info(s"stopping self $self")
    super.postStop()
  }

  override def receive: Receive = super.receive orElse {
    case GetQuery =>
      log.info("Creating Query")
      sender() ! this.query
      log.info(s"Query is: ${this.query.toString}")

    case GetAllRecords =>
      log.debug(s"${sender()} asked for AllRecords")
      sender() ! this.allRecords

    case GetMonitorFactories =>
      sender() ! this.monitorFactories

    case GetRequirementChange =>
      sender() ! Set.empty

    case event: Event =>
      log.debug(s"$self received EVENT: $event")
      this.monitors.foreach(monitor => monitor.onEventEmit(event, this.transitionStatus))

    case SetStatus(status) =>
      log.info(s"transition status changed to $status")
      this.transitionStatus = status

    case SetQosMonitors =>
      if(monitorFactories.isEmpty) {
        log.info(s"${self} Setting QoS Monitors")
        val latencyMonitorFactory = PathLatencyMonitorFactory(query, Some(allRecords.recordLatency))
        val hopsMonitorFactory = MessageHopsMonitorFactory(query, Some(allRecords.recordMessageHops), allRecords.recordProcessingNodes)
        val loadMonitorFactory = LoadMonitorFactory(query, Some(allRecords.recordAverageLoad))
        val frequencyMonitorFactory = AverageFrequencyMonitorFactory(query, Some(allRecords.recordFrequency))
        val recordTransitionStatusFactory = TransitionMonitorFactory(query, allRecords.recordTransitionStatus)
        val messageOverheadMonitorFactory = MessageOverheadMonitorFactory(query, allRecords.recordMessageOverhead)
        val networkUsageMonitorFactory = NetworkUsageMonitorFactory(query, allRecords.recordNetworkUsage)
        val publishingRateMonitorFactory = PublishingRateMonitorFactory(query, allRecords.recordPublishingRate)

        monitorFactories = Array(latencyMonitorFactory, hopsMonitorFactory,
          loadMonitorFactory, frequencyMonitorFactory, messageOverheadMonitorFactory, networkUsageMonitorFactory,
          recordTransitionStatusFactory, publishingRateMonitorFactory)
        this.monitors = monitorFactories.map(f => f.createNodeMonitor)
        //log.info(s"Monitors set: ${this.monitors.toString}")
        log.info(s"Setting QoS Monitors set. AllRecords are: ${this.allRecords.getValues}")
      }
    case SetStreams(streams) =>
      this.eventStreams = streams.asInstanceOf[Seq[Vector[Stream1[Any]]]]
      log.info("Event streams received")
  }

  def queryFunction(): Query
}

object Consumer {
  case object GetQuery
  case object GetAllRecords
  case object GetMonitorFactories
  case object GetRequirementChange
  case class SetStatus(status: Int)
  case object SetQosMonitors
  //case class SetStreams(streams: (Vector[Stream1[MobilityData]],Vector[Stream1[Int]]))
  case class SetStreams(streams: Seq[Any])
}