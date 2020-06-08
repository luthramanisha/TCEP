package tcep.graph.nodes

import java.time.Duration

import akka.actor.ActorRef
import com.espertech.esper.client._
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.nodes.JoinNode._
import tcep.graph.nodes.traits.EsperEngine._
import tcep.graph.nodes.traits.TransitionModeNames.{apply => _, _}
import tcep.graph.nodes.traits._
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}
import tcep.placement.HostInfo

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

/**
  * Handling of [[tcep.data.Queries.JoinQuery]] is done by JoinNode.
  *
  * @see [[QueryGraph]]
  **/

case class JoinNode(transitionConfig: TransitionConfig, hostInfo: HostInfo, backupMode: Boolean, mainNode: Option[ActorRef], query: JoinQuery, createdCallback: Option[CreatedCallback], eventCallback: Option[EventCallback], isRootOperator: Boolean, parents: ActorRef*)
  extends BinaryNode with EsperEngine {

  var parentNode1 = parents.head
  var parentNode2 = parents.last
  override val esperServiceProviderUri: String = name
  var esperInitialized = false

  override def preStart(): Unit = {
    super.preStart()

    val type1 = addEventType("sq1", createArrayOfNames(query.sq1), createArrayOfClasses(query.sq1))
    val type2 = addEventType("sq2", createArrayOfNames(query.sq2), createArrayOfClasses(query.sq2))
    val epStatement: EPStatement = createEpStatement(s"select * from " +
      s"sq1.${createWindowEplString(query.w1)} as sq1," +
      s"sq2.${createWindowEplString(query.w2)} as sq2 ")

    val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
      /**
        * @author Niels
        */
      val values1: Array[Any] = eventBean.get("sq1").asInstanceOf[Array[Any]]
      val values2: Array[Any] = eventBean.get("sq2").asInstanceOf[Array[Any]]
      val values = values1.tail ++ values2.tail

        val monitoringData1 = values1.head.asInstanceOf[MonitoringData]
        val monitoringData2 = values2.head.asInstanceOf[MonitoringData]

      //log.info(s"event streams: \n ${values1.toList.tail} \n ${values2.toList.tail}")

      val event: Event = values.length match {
        case 2 =>
          val res = Event2(values(0), values(1))
          mergeMonitoringData(res, monitoringData1, monitoringData2, log)
        case 3 =>
          val res = Event3(values(0), values(1), values(2))
          mergeMonitoringData(res, monitoringData1, monitoringData2, log)
        case 4 =>
          val res = Event4(values(0), values(1), values(2), values(3))
          mergeMonitoringData(res, monitoringData1, monitoringData2, log)
        case 5 =>
          val res = Event5(values(0), values(1), values(2), values(3), values(4))
          mergeMonitoringData(res, monitoringData1, monitoringData2, log)
        case 6 =>
          val res = Event6(values(0), values(1), values(2), values(3), values(4), values(5))
          mergeMonitoringData(res, monitoringData1, monitoringData2, log)
      }
      log.debug(s"creating new event $event with merged monitoringData")
      emitEvent(event)
    })
    epStatement.addListener(updateListener)
    esperInitialized = true


  }

  override def childNodeReceive: Receive = super.childNodeReceive orElse {

    case event: Event if p1List.contains(sender()) && esperInitialized => event match {
      case Event1(e1) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case event: Event if p2List.contains(sender()) && esperInitialized => event match {
      case Event1(e1) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case unhandledMessage => log.info(s"${self.path.name} received msg ${unhandledMessage.getClass} from ${sender()}, esper initialized: $esperInitialized")
  }

  override def maxWindowTime(): Duration = {
    def windowTime(w: Window): Duration = w match {
      case SlidingTime(seconds) => Duration.ofSeconds(seconds)
      case TumblingTime(seconds) => Duration.ofSeconds(seconds)
      case SlidingInstances(instances) => Duration.ofNanos(instances * eventIntervalMicroseconds * 1000)
      case TumblingInstances(instances) => Duration.ofNanos(instances * eventIntervalMicroseconds * 1000)
    }

    val w1 = windowTime(query.w1)
    val w2 = windowTime(query.w2)
    if (w1.compareTo(w2) > 0) w1 else w2
  }

  override def postStop(): Unit = {
    destroyServiceProvider()
    super.postStop()
  }
}

object JoinNode {

  def createWindowEplString(window: Window): String = window match {
    case SlidingTime(seconds) => s"win:time($seconds)"
    case TumblingTime(seconds) => s"win:time_batch($seconds)"
    case SlidingInstances(instances) => s"win:length($instances)"
    case TumblingInstances(instances) => s"win:length_batch($instances)"
  }

}
