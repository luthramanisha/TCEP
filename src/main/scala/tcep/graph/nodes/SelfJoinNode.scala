package tcep.graph.nodes

import java.time.Duration

import akka.actor.{ActorLogging, ActorRef}
import com.espertech.esper.client._
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.nodes.JoinNode._
import tcep.graph.nodes.traits.EsperEngine._
import tcep.graph.nodes.traits.TransitionModeNames.{apply => _, _}
import tcep.graph.nodes.traits._
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}
import tcep.placement.HostInfo

import scala.concurrent.duration.{FiniteDuration, MICROSECONDS, SECONDS}

/**
  * Handling of [[tcep.data.Queries.SelfJoinQuery]] is done by SelfJoinNode.
  *
  * @see [[QueryGraph]]
  **/

case class SelfJoinNode(transitionConfig: TransitionConfig,
                        hostInfo: HostInfo,
                        backupMode: Boolean,
                        mainNode: Option[ActorRef],
                        query: SelfJoinQuery,
                        createdCallback: Option[CreatedCallback],
                        eventCallback: Option[EventCallback],
                        isRootOperator: Boolean,
                        _parentNode: ActorRef*
                       )
  extends UnaryNode with EsperEngine with ActorLogging {

  var parentNode: ActorRef = _parentNode.head
  override val esperServiceProviderUri: String = name
  var esperInitialized = false

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event if parentsList.contains(sender()) && esperInitialized => event match {
      case Event1(e1) => sendEvent("sq", Array(toAnyRef(event.monitoringData), toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
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

  override def preStart(): X = {
    super.preStart()

    addEventType("sq", createArrayOfNames(query.sq), createArrayOfClasses(query.sq))
    val epStatement: EPStatement = createEpStatement(
      s"select * from " +
        s"sq.${createWindowEplString(query.w1)} as lhs, " +
        s"sq.${createWindowEplString(query.w2)} as rhs")

    val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {

      val values1: Array[Any] = eventBean.get("lhs").asInstanceOf[Array[Any]]
      val values2: Array[Any] = eventBean.get("rhs").asInstanceOf[Array[Any]]
      val values = values1.tail ++ values2.tail
      val monitoringData = values1.head.asInstanceOf[MonitoringData]

      val event: Event = values.length match {
        case 2 => Event2(values(0), values(1))
        case 3 => Event3(values(0), values(1), values(2))
        case 4 => Event4(values(0), values(1), values(2), values(3))
        case 5 => Event5(values(0), values(1), values(2), values(3), values(4))
        case 6 => Event6(values(0), values(1), values(2), values(3), values(4), values(5))
      }
      event.monitoringData = monitoringData
      //log.info(s"creating new event $event")
      emitEvent(event)
    })

    epStatement.addListener(updateListener)
    esperInitialized = true
  }


}
