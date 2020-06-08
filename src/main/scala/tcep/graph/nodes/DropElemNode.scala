package tcep.graph.nodes

import akka.actor.ActorRef
import tcep.data.Events._
import tcep.data.Queries._
import tcep.factories.NodeFactory
import tcep.graph.nodes.traits._
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}
import tcep.machinenodes.helper.actors.{CreateRemoteOperator, RemoteOperatorCreated}
import tcep.placement.HostInfo
import tcep.utils.TCEPUtils

import scala.concurrent.Future

/**
  * Handling of [[tcep.data.Queries.DropElemQuery]] is done by DropElemNode.
  *
  * @see [[QueryGraph]]
  **/

case class DropElemNode(transitionConfig: TransitionConfig,
                        hostInfo: HostInfo,
                        backupMode: Boolean,
                        mainNode: Option[ActorRef],
                        query: DropElemQuery,
                        createdCallback: Option[CreatedCallback],
                        eventCallback: Option[EventCallback],
                        isRootOperator: Boolean,
                        _parentNode: ActorRef*
                       )
  extends UnaryNode {

  var parentNode: ActorRef = _parentNode.head
  val elemToBeDropped: Int = query match {
    case DropElem1Of2(_, _) => 1
    case DropElem1Of3(_, _) => 1
    case DropElem1Of4(_, _) => 1
    case DropElem1Of5(_, _) => 1
    case DropElem1Of6(_, _) => 1
    case DropElem2Of2(_, _) => 2
    case DropElem2Of3(_, _) => 2
    case DropElem2Of4(_, _) => 2
    case DropElem2Of5(_, _) => 2
    case DropElem2Of6(_, _) => 2
    case DropElem3Of3(_, _) => 3
    case DropElem3Of4(_, _) => 3
    case DropElem3Of5(_, _) => 3
    case DropElem3Of6(_, _) => 3
    case DropElem4Of4(_, _) => 4
    case DropElem4Of5(_, _) => 4
    case DropElem4Of6(_, _) => 4
    case DropElem5Of5(_, _) => 5
    case DropElem5Of6(_, _) => 5
    case DropElem6Of6(_, _) => 6
  }

  def handleEvent2(e1: Any, e2: Any): Event1 = elemToBeDropped match {
    case 1 => Event1(e2)
    case 2 => Event1(e1)
  }

  def handleEvent3(e1: Any, e2: Any, e3: Any): Event2 = elemToBeDropped match {
    case 1 => Event2(e2, e3)
    case 2 => Event2(e1, e3)
    case 3 => Event2(e1, e2)
  }

  def handleEvent4(e1: Any, e2: Any, e3: Any, e4: Any): Event3 = elemToBeDropped match {
    case 1 => Event3(e2, e3, e4)
    case 2 => Event3(e1, e3, e4)
    case 3 => Event3(e1, e2, e4)
    case 4 => Event3(e1, e2, e3)
  }

  def handleEvent5(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any): Event4 = elemToBeDropped match {
    case 1 => Event4(e2, e3, e4, e5)
    case 2 => Event4(e1, e3, e4, e5)
    case 3 => Event4(e1, e2, e4, e5)
    case 4 => Event4(e1, e2, e3, e5)
    case 5 => Event4(e1, e2, e3, e4)
  }

  def handleEvent6(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any, e6: Any): Event5 = elemToBeDropped match {
    case 1 => Event5(e2, e3, e4, e5, e6)
    case 2 => Event5(e1, e3, e4, e5, e6)
    case 3 => Event5(e1, e2, e4, e5, e6)
    case 4 => Event5(e1, e2, e3, e5, e6)
    case 5 => Event5(e1, e2, e3, e4, e6)
    case 6 => Event5(e1, e2, e3, e4, e5)
  }

  override def childNodeReceive: Receive = super.childNodeReceive orElse {

    case event: Event if parentsList.contains(sender()) =>
      val newEvent = event match {
        case Event1(_) => sys.error("Panic! Control flow should never reach this point!")
        case Event2(e1, e2) => handleEvent2(e1, e2)
        case Event3(e1, e2, e3) => handleEvent3(e1, e2, e3)
        case Event4(e1, e2, e3, e4) => handleEvent4(e1, e2, e3, e4)
        case Event5(e1, e2, e3, e4, e5) => handleEvent5(e1, e2, e3, e4, e5)
        case Event6(e1, e2, e3, e4, e5, e6) => handleEvent6(e1, e2, e3, e4, e5, e6)
      }
      newEvent.monitoringData = event.monitoringData
      emitEvent(newEvent)

    case unhandledMessage => log.info(s"${self.path.name} received msg ${unhandledMessage.getClass} from ${sender()}")
  }
}