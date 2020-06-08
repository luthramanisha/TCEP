package tcep.graph.transition.mapek.lightweight


import akka.actor.{ActorRef, Address, Cancellable}
import akka.pattern.ask
import tcep.data.Queries.{FrequencyRequirement, LatencyRequirement, LoadRequirement, MessageHopsRequirement}
import tcep.graph.transition.MAPEK.{AddRequirement, GetOperators, GetTransitionStatus, IsDeploymentComplete, RemoveRequirement}
import tcep.graph.transition.MonitorComponent
import tcep.graph.transition.mapek.lightweight.LightweightAnalyzer.NodeUnreachable
import tcep.graph.transition.mapek.lightweight.LightweightKnowledge.UpdatePerformance
import tcep.graph.transition.mapek.lightweight.LightweightMAPEK.GetConsumer
import tcep.machinenodes.consumers.Consumer.GetAllRecords
import tcep.simulation.tcep.AllRecords

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
class LightweightMonitor(mapek: LightweightMAPEK/*, var allRecords: AllRecords*/) extends MonitorComponent{

  var updatePerformanceScheduler: Cancellable = _
  var checkOperatorsScheduler: Cancellable = _
  var testSchedule: Cancellable = _
  //var allRecords: AllRecords =  AllRecords()

  override def preStart(): Unit = {
    super.preStart()
    log.info("Starting Lightweight Monitor")
    updatePerformanceScheduler = this.context.system.scheduler.schedule(5 seconds, mapek.samplingInterval)(f = updatePerformance())
    //checkOperatorsScheduler = this.context.system.scheduler.schedule(1 minute, 30 seconds)(f = checkOperatorHostState())
    //testSchedule = this.context.system.scheduler.schedule(1 minute, 30 seconds)(f = testLogging())
    log.info(s"Registered updatePerformance Scheduler $updatePerformanceScheduler")
  }

  override def postStop(): Unit = {
    super.postStop()
    updatePerformanceScheduler.cancel()
    checkOperatorsScheduler.cancel()
  }

  override def receive: Receive = {
    case r: AddRequirement =>
      mapek.knowledge ! r
    case r: RemoveRequirement =>
      mapek.knowledge ! r
  }

  def updatePerformance(): Unit = {
    log.info(s"Update Performance called.")
    for {
      consumer <- (mapek.knowledge ? GetConsumer).mapTo[ActorRef]
      allRecords <- (consumer ? GetAllRecords).mapTo[AllRecords]
      deploymentComplete <- (mapek.knowledge ? IsDeploymentComplete).mapTo[Boolean]
      transitionStatus <- (mapek.knowledge ? GetTransitionStatus).mapTo[Int]
      if deploymentComplete && allRecords.allDefined// && transitionStatus == 0
    } yield {
      val currentLatency: Double = allRecords.recordLatency.lastMeasurement.get.toMillis
      val currentMsgHops: Int = allRecords.recordMessageHops.lastMeasurement.get
      val currentSystemLoad: Double = allRecords.recordAverageLoad.lastLoadMeasurement.get
      val currentArrivalRate: Int = allRecords.recordFrequency.lastMeasurement.get
      val currentPublishingRate: Double = allRecords.recordPublishingRate.lastRateMeasurement.get
      val msgOverhead: Long = allRecords.recordMessageOverhead.lastEventOverheadMeasurement.get
      val networkUsage: Double = allRecords.recordNetworkUsage.lastUsageMeasurement.get
      var measurements = new ListBuffer[(String,Double)]
      measurements += ((LatencyRequirement.name, currentLatency))
      measurements += ((MessageHopsRequirement.name, currentMsgHops))
      measurements += ((LoadRequirement.name, currentSystemLoad))
      measurements += ((FrequencyRequirement.name, currentArrivalRate))
      measurements += (("PublishRate", currentPublishingRate))
      measurements += (("MsgOverhead", msgOverhead))
      measurements += (("NetworkUsage", networkUsage))
      mapek.knowledge ! UpdatePerformance(measurements.toList)
    }
  }

  /**
    * check if any operator hosts are down; executed periodically
    */
  def checkOperatorHostState(): Unit = {
    for {
      //deploymentComplete <- (mapek.knowledge ? IsDeploymentComplete).mapTo[Boolean]
      //if deploymentComplete
      operators <- (mapek.knowledge ? GetOperators).mapTo[List[ActorRef]]
    } yield {

      log.info("checking OperatorHost state")
      // check if an operator of the graph has an unreachable host (and no backup)
      // if yes, notify analyzer to trigger transition and/or placement algorithm execution
      val unreachableNodes: Set[Address] = cluster.state.unreachable.map(m => m.address)
      operators.foreach { op =>
        val host: Address = op.path.address
        if(unreachableNodes.contains(host)) {
          log.info(s"host $host of operator $op is unreachable, notifying analyzer of Network Change")
          mapek.analyzer ! NodeUnreachable
        }
      }
    }
  }
}

object LightweightMonitor {
}