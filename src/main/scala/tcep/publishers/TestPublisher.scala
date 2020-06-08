package tcep.publishers

import tcep.data.Events.Event
import tcep.graph.nodes.traits.Node._

case class TestPublisher() extends Publisher {

  override def receive: Receive = {
    case Subscribe(s) =>
      super.receive(Subscribe(s))
    case e: Event =>
      subscribers.foreach(_ ! e)
    case message =>
      subscribers.foreach(_ ! message)
  }

}
