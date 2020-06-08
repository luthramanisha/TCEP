package tcep.graph.nodes

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Cancellable}
import tcep.data.Events.{Event, Event1, Event6}
import tcep.data.Queries.SlidingWindowQuery
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.nodes.traits.{TransitionConfig, UnaryNode}
import tcep.graph.{CreatedCallback, EventCallback}
import tcep.placement.HostInfo
import tcep.simulation.tcep.LinearRoadDataNew

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

case class SlidingWindowNode(
                            transitionConfig: TransitionConfig,
                            hostInfo: HostInfo,
                            backupMode: Boolean,
                            mainNode: Option[ActorRef],
                            query: SlidingWindowQuery,
                            createdCallback: Option[CreatedCallback],
                            eventCallback: Option[EventCallback],
                            isRootOperator: Boolean,
                            _parentNode: ActorRef*
                            ) extends UnaryNode {

  var parentNode: ActorRef = _parentNode.head
  var storage: ListBuffer[(Double, Double)] = ListBuffer.empty[(Double, Double)]
  var lastEmit: Double = System.currentTimeMillis().toDouble
  val rate = 2
  var avgEmitter: Cancellable = _
  //var avgEmitter = context.system.scheduler.schedule(FiniteDuration(0, TimeUnit.SECONDS), FiniteDuration(rate, TimeUnit.SECONDS))(this.emitAvg)

  override def preStart(): Unit = {
    super.preStart()
    avgEmitter = context.system.scheduler.schedule(FiniteDuration(0, TimeUnit.SECONDS), FiniteDuration(rate, TimeUnit.SECONDS))(this.emitAvg)
  }

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      val s = sender()
      if (this.parentsList.contains(s)) {
        event match {
          case Event6(e1, e2, e3, e4, e5, e6) =>
            this.store(List(e1.asInstanceOf[LinearRoadDataNew], e2.asInstanceOf[LinearRoadDataNew], e3.asInstanceOf[LinearRoadDataNew], e4.asInstanceOf[LinearRoadDataNew], e5.asInstanceOf[LinearRoadDataNew], e6.asInstanceOf[LinearRoadDataNew]))
        }
        val eventData = if (this.storage.size > 0) {
           LinearRoadDataNew(-1, this.query.sectionFilter.get, -1, -1, false, Some(this.getWindow()))
        } else {
           LinearRoadDataNew(-1, this.query.sectionFilter.get, -1, -1, false, Some(List(0)))
        }
        val avgEvent = Event1(eventData)
        avgEvent.init()
        avgEvent.copyMonitoringData(event.monitoringData)
        emitEvent(avgEvent)
      }
    case unhandledMessage => log.info(s"${self.path.name} received msg ${unhandledMessage.getClass} from ${sender()}")
  }

  def emitAvg(): Unit = {
    if (this.storage.size > 0) {
      val eventData = LinearRoadDataNew(-1, this.query.sectionFilter.get, -1, -1, false, Some(this.getWindow()))
      val avgEvent = Event1(eventData)
      avgEvent.init()
      //TODO how to include monitoringData from parent events?
      emitEvent(avgEvent)
    } else {
      val eventData = LinearRoadDataNew(-1, this.query.sectionFilter.get, -1, -1, false, Some(List(0)))
      val avgEvent = Event1(eventData)
      avgEvent.init()
      emitEvent(avgEvent)
    }
  }

  def store(dataList: List[LinearRoadDataNew]) = {
    val currentTime = System.currentTimeMillis() / 1000.0d
    if (this.query.sectionFilter.isDefined) {
      for (value <- dataList) {
        if (value.section == this.query.sectionFilter.get) {
          this.storage += (currentTime -> value.speed)
        }
      }
    }
  }

  def getWindow(): List[Double] = {
    val currentTime = System.currentTimeMillis() / 1000.0d //seconds
    val removeThreshold = currentTime-this.query.windowSize
    while (this.storage.size > 0 && this.storage.head._1 < removeThreshold)
      this.storage = this.storage.tail
    var out = ListBuffer.empty[Double]
    for (event <- this.storage)
      out += (event._2)
    out.toList
  }

        /*def store(dataList: List[LinearRoadDataNew]): List[LinearRoadDataNew] = {
    val currentTime = System.currentTimeMillis() / 1000.0d // seconds
    var out: ListBuffer[LinearRoadDataNew] = ListBuffer.empty[LinearRoadDataNew]
    if (this.query.sectionFilter.isDefined) {
      for (value <- dataList){
        if (value.section == this.query.sectionFilter.get) {
          this.storage += (currentTime -> value.speed)
        }
        if (value.change && value.section-1 == this.query.sectionFilter.get) {
          value.dataWindow = Some(this.getWindow())
          out += value
        }
      }
      out.toList
    } else {
      List()
    }
  }

  def getWindow(): List[Double] = {
    val currentTime = System.currentTimeMillis() / 1000.0d //seconds
    val removeThreshold = currentTime-this.query.windowSize
    while (this.storage.size > 0 && this.storage.head._1 < removeThreshold)
      this.storage = this.storage.tail
    var out = ListBuffer.empty[Double]
    for (event <- this.storage)
      out += (event._2)
    out.toList
  }*/

  override def maxWindowTime(): Duration = {
    Duration.ofSeconds(0)
  }
}