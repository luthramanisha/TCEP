package tcep.graph.nodes

import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorRef}
import akka.util.Timeout
import com.espertech.esper.client._
import tcep.data.Events._
import tcep.data.Queries._
import tcep.graph.nodes.traits.Node.Subscribe
import tcep.graph.nodes.traits.TransitionModeNames._
import tcep.graph.nodes.traits._
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}
import tcep.placement.HostInfo
import tcep.publishers.Publisher._
import tcep.utils.TCEPUtils

import scala.concurrent.duration.FiniteDuration

/**
  * Handling of [[tcep.data.Queries.SequenceQuery]] is done by SequenceNode.
  *
  * @see [[QueryGraph]]
  **/

case class SequenceNode(transitionConfig: TransitionConfig,
                        hostInfo: HostInfo,
                        backupMode: Boolean,
                        mainNode: Option[ActorRef],
                        query: SequenceQuery,
                        createdCallback: Option[CreatedCallback],
                        eventCallback: Option[EventCallback],
                        isRootOperator: Boolean,
                        publishers: ActorRef*
) extends LeafNode with EsperEngine with ActorLogging {

  override val esperServiceProviderUri: String = name
  var esperInitialized = false
  implicit val resolveTimeout = Timeout(60, TimeUnit.SECONDS)

  var subscription1Acknowledged: Boolean = false
  var subscription2Acknowledged: Boolean = false

  override def preStart(): Unit = {
    super.preStart()
    assert(publishers.size == 2)
    val type1 = addEventType("sq1", SequenceNode.createArrayOfNames(query.s1), SequenceNode.createArrayOfClasses(query.s1))
    val type2 = addEventType("sq2", SequenceNode.createArrayOfNames(query.s2), SequenceNode.createArrayOfClasses(query.s2))
    val epStatement: EPStatement = createEpStatement("select * from pattern [every (sq1=sq1 -> sq2=sq2)]")

      val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {

        val values1: Array[Any] = eventBean.get("sq1").asInstanceOf[Array[Any]]
        val values2: Array[Any] = eventBean.get("sq2").asInstanceOf[Array[Any]]
        val values = values1.tail ++ values2.tail
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
      })
      epStatement.addListener(updateListener)
      esperInitialized = true

  }

  override def subscribeToParents(): Unit = {
    log.info(s"subscribing for events from $publishers")

    implicit val resolveTimeout = Timeout(10, TimeUnit.SECONDS)
    for {
      ack1 <- TCEPUtils.guaranteedDelivery(context, publishers.head, Subscribe(self))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
      ack2 <- TCEPUtils.guaranteedDelivery(context, publishers.last, Subscribe(self))(blockingIoDispatcher).mapTo[AcknowledgeSubscription]
    } yield {
      log.info(s"subscribed for events from $publishers")
      context.system.scheduler.scheduleOnce(FiniteDuration(100, TimeUnit.MILLISECONDS), self, Created) // delay sending Created so all children can subscribe to avoid failing tests
    }
  }

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case AcknowledgeSubscription() if sender().equals(publishers.head) =>
      subscription1Acknowledged = true
      if (subscription2Acknowledged) emitCreated()
    case AcknowledgeSubscription() if sender().equals(publishers(1)) =>
      subscription2Acknowledged = true
      if (subscription1Acknowledged) emitCreated()
    case event: Event if sender().equals(publishers.head) && esperInitialized => event match {
      case Event1(e1) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq1", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case event: Event if sender().equals(publishers(1)) && esperInitialized => event match {
      case Event1(e1) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq2", Array(toAnyRef(event.monitoringData), toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case unhandledMessage => log.info(s"unhandled message $unhandledMessage, esper initialized: $esperInitialized")
  }

  def getParentOperators(): Seq[ActorRef] = publishers

  override def postStop(): Unit = {
    destroyServiceProvider()
    super.postStop()
  }

}

object SequenceNode {
  def createArrayOfNames(noReqStream: NStream): Array[String] = noReqStream match {
    case _: NStream1[_] => Array("e1")
    case _: NStream2[_, _] => Array("e1", "e2")
    case _: NStream3[_, _, _] => Array("e1", "e2", "e3")
    case _: NStream4[_, _, _, _] => Array("e1", "e2", "e3", "e4")
    case _: NStream5[_, _, _, _, _] => Array("e1", "e2", "e3", "e4", "e5")
  }

  def createArrayOfClasses(noReqStream: NStream): Array[Class[_]] = {
    val clazz: Class[_] = classOf[AnyRef]
    noReqStream match {
      case _: NStream1[_] => Array(clazz)
      case _: NStream2[_, _] => Array(clazz, clazz)
      case _: NStream3[_, _, _] => Array(clazz, clazz, clazz)
      case _: NStream4[_, _, _, _] => Array(clazz, clazz, clazz, clazz)
      case _: NStream5[_, _, _, _, _] => Array(clazz, clazz, clazz, clazz, clazz)
    }
  }
}
