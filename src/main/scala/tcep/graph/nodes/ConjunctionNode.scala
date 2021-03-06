package tcep.graph.nodes

import akka.actor.ActorRef
import com.espertech.esper.client._
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.nodes.traits.EsperEngine._
import tcep.graph.nodes.traits._
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}
import tcep.placement.HostInfo

/**
  * Handling of [[tcep.data.Queries.ConjunctionQuery]] is done by ConjunctionNode.
  *
  * @see [[QueryGraph]]
  **/
case class ConjunctionNode(transitionConfig: TransitionConfig, hostInfo: HostInfo, backupMode: Boolean, mainNode: Option[ActorRef], query: ConjunctionQuery, createdCallback: Option[CreatedCallback], eventCallback: Option[EventCallback], isRootOperator: Boolean,  parents: ActorRef*)
  extends BinaryNode with EsperEngine {

  var parentNode1 = parents.head
  var parentNode2 = parents.last

  override val esperServiceProviderUri: String = name
  var esperInitialized = false
  // override default configuration here to avoid esper spawning an (in our case) unneeded timer thread for each operator,
  // which is NOT removed after shutting down the actor
  override val configuration = new Configuration
  configuration.getEngineDefaults.getThreading.setInternalTimerEnabled(false)

  override def preStart(): Unit = {
    super.preStart()

    val type1 = addEventType("sq1", createArrayOfNames(query.sq1), createArrayOfClasses(query.sq1))
    val type2 = addEventType("sq2", createArrayOfNames(query.sq2), createArrayOfClasses(query.sq2))
    val epStatement: EPStatement = createEpStatement("select * from pattern [every (sq1=sq1 and sq2=sq2)]")
    val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
      /**
        * @author Niels
        */
      val values1: Array[Any] = eventBean.get("sq1").asInstanceOf[Array[Any]]
      val values2: Array[Any] = eventBean.get("sq2").asInstanceOf[Array[Any]]
      try {
        val values = values1.tail ++ values2.tail
        //log.info(s"received eventBean: \n values1: $values1 \n values2: $values2")
        val monitoringData1 = values1.head.asInstanceOf[MonitoringData]
        val monitoringData2 = values2.head.asInstanceOf[MonitoringData]

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
        emitEvent(event)
      } catch {
        case e: Throwable => log.error(e, s"failed to merge events from event beans: \n $values1 \n $values2")
      }
    })

    epStatement.addListener(updateListener)
    esperInitialized = true
    log.info(s"created $self with parents \n $parentNode1 and \n $parentNode2")
  }

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event if p1List.contains(sender()) => event match {
      case Event1(e1) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case event: Event if p2List.contains(sender()) => event match {
      case Event1(e1) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case unhandledMessage => log.info(s"${self.path.name} received msg ${unhandledMessage.getClass} from ${sender()}")  }


  override def postStop(): Unit = {
    destroyServiceProvider()
    super.postStop()
  }

}
