package tcep.akkamailbox

import akka.actor.{Actor, Props}
import tcep.data.Events.Event1
import tcep.machinenodes.helper.actors.{MeasurementMessage, TransitionControlMessage, VivaldiCoordinatesMessage}
import tcep.utils.SpecialStats
import tcep.{MultiJVMTestSetup, TCEPMultiNodeConfig}

class TCEPPriorityMailboxMultiJvmNode1 extends TCEPPriorityMailboxMultiNodeTestSpec
class TCEPPriorityMailboxMultiJvmNode2 extends TCEPPriorityMailboxMultiNodeTestSpec

abstract class TCEPPriorityMailboxMultiNodeTestSpec extends MultiJVMTestSetup(2) {

  import TCEPMultiNodeConfig._

  case class DummyTransitionMsg(s: String) extends TransitionControlMessage
  case class DummyVivaldiMsg(s: String) extends VivaldiCoordinatesMessage
  case class DummyMeasurementMsg(s: String) extends MeasurementMessage

  val transitionMsg = DummyTransitionMsg(s"transition msg (prio 0")
  val measurementMsg = DummyMeasurementMsg("placementMeasurement msg (prio 1")
  val eventMsg = Event1("event (prio 2)")

  class Printer extends Actor {

    override def preStart(): Unit = {
      super.preStart()
      println(s"actorref: $self")
      Thread.sleep(1000) // wait so that mailbox can fill up
    }

    def receive = {

      case x ⇒ {
        val s = sender()
        println(SpecialStats.getTimestamp + ": " + this.context.props.mailbox + " sender: " + s + " " + x.toString)
        Thread.sleep(500)
        s ! x
      }
    }
  }

  "TCEPPriorityMailbox" must {
    "deliver messages to local actor according to priority" in {
      runOn(node1) {
        println("<-- this is node1")
        val withPrio = system.actorOf(Props(new Printer).withMailbox("prio-mailbox"), "withPrio")

        withPrio ! eventMsg
        withPrio ! measurementMsg
        withPrio ! transitionMsg
        expectMsg(transitionMsg)
        expectMsg(measurementMsg)
        expectMsg(eventMsg)
      }

      runOn(client) {
        val noPrio = system.actorOf(Props(new Printer), "noPrio")
        noPrio ! eventMsg
        noPrio ! measurementMsg
        noPrio ! transitionMsg
        expectMsg(eventMsg)
        expectMsg(measurementMsg)
        expectMsg(transitionMsg)

      }

      testConductor.enter("test1 complete")
    }
  }

  "TCEPPriorityMailbox" must {
    "deliver messages to a named actor (see TCEPMultiNodeConfig) according to their priority" in {

      runOn(client) {
        // use name "PrioActor" which has been configured to use priority mailbox in TCEPMultiNodeConfig -> no explicit mailbox type assignment
        val withPrio = system.actorOf(Props(new Printer), "PrioActor")

        withPrio ! eventMsg
        withPrio ! measurementMsg
        withPrio ! transitionMsg
        expectMsg(transitionMsg)
        expectMsg(measurementMsg)
        expectMsg(eventMsg)      }

      testConductor.enter("test2 complete")
    }
  }

}