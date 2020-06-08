package tcep.graph.nodes

import akka.actor.ActorRef
import tcep.data.Events._
import tcep.data.Queries.AverageQuery
import tcep.factories.NodeFactory
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.nodes.traits.{TransitionConfig, UnaryNode}
import tcep.graph.{CreatedCallback, EventCallback}
import tcep.machinenodes.helper.actors.{CreateRemoteOperator, RemoteOperatorCreated}
import tcep.placement.HostInfo
import tcep.simulation.tcep.MobilityData
import tcep.utils.{SpecialStats, TCEPUtils}

import scala.concurrent.Future

/**
  * Operator that calculates the average of the values contained in the received event
  */
case class AverageNode(
                        transitionConfig: TransitionConfig,
                        hostInfo: HostInfo,
                        backupMode: Boolean,
                        mainNode: Option[ActorRef],
                        query: AverageQuery,
                        createdCallback: Option[CreatedCallback],
                        eventCallback: Option[EventCallback],
                        isRootOperator: Boolean,
                        _parentNode: ActorRef*
                      ) extends UnaryNode {

  var parentNode: ActorRef = _parentNode.head
  /**
    * calculate the average of the given dataList of Ints, Doubles or MobilityData items
    * @param dataList
    * @return the average of any non-Double.NaN values, Double.Nan if none
    */
  def calculateAvg(dataList: List[Any]): MobilityData = {
    def extract(entry: Any) : Option[Double] = entry match {
      case m: MobilityData =>
        log.debug(s"received event with data $m")
        if (query.sectionFilter.isDefined && query.sectionFilter.get == m.publisherSection && m.speed != Double.MinValue) Some(m.speed)
        else if (query.sectionFilter.isDefined) None
        else Some(m.speed)
      case d: Double => Some(d)
      case d: Int => Some(d.toDouble)
      case other => throw new IllegalArgumentException(s"unknown data type: $other")
    }
    val values = dataList.flatMap(extract)
    //SpecialStats.log(this.getClass.toString, "avgEvent", s"data: $dataList, sectionFilter: ${query.sectionFilter}, values: $values, avg: ${values.sum / values.length}")
    if(values.nonEmpty) MobilityData(query.sectionFilter.getOrElse(-1), values.sum / values.length)
    else MobilityData(query.sectionFilter.getOrElse(-1), Double.MinValue)// Double.MinValue if no values from section so following joins / conjunctions still receive events
  }

  override def childNodeReceive: Receive = super.childNodeReceive orElse {

    case event: Event if parentsList.contains(sender()) =>

      //log.debug(s"averageNode received event: ${event}")
      val value = event match {
        case Event1(e1) => calculateAvg(List(e1))
        case Event2(e1, e2) => calculateAvg(List(e1, e2))
        case Event3(e1, e2, e3) => calculateAvg(List(e1, e2, e3))
        case Event4(e1, e2, e3, e4) => calculateAvg(List(e1, e2, e3, e4))
        case Event5(e1, e2, e3, e4, e5) => calculateAvg(List(e1, e2, e3, e4, e5))
        case Event6(e1, e2, e3, e4, e5, e6) => calculateAvg(List(e1, e2, e3, e4, e5, e6))
      }

      val averageEvent: Event1 = Event1(value)
      averageEvent.monitoringData = event.monitoringData
      //log.debug(s"average event with monitoring data: $averageEvent")
      emitEvent(averageEvent)

    case unhandledMessage => log.info(s"${self.path.name} received msg ${unhandledMessage.getClass} from ${sender()}")
  }

}
