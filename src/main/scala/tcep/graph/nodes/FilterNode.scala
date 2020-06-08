package tcep.graph.nodes

import java.io.{File, PrintStream}

import akka.actor.ActorRef
import tcep.data.Events._
import tcep.data.Queries._
import tcep.factories.NodeFactory
import tcep.graph.nodes.traits._
import tcep.graph.{CreatedCallback, EventCallback, QueryGraph}
import tcep.machinenodes.helper.actors.{CreateRemoteOperator, RemoteOperatorCreated}
import tcep.placement.HostInfo
import tcep.simulation.tcep.SimulationRunner.logger
import tcep.utils.TCEPUtils

import scala.concurrent.Future

/**
  * Handling of [[tcep.data.Queries.FilterQuery]] is done by FilterNode.
  *
  * @see [[QueryGraph]]
  **/

case class FilterNode(transitionConfig: TransitionConfig,
                      hostInfo: HostInfo,
                      backupMode: Boolean,
                      mainNode: Option[ActorRef],
                      query: FilterQuery,
                      createdCallback: Option[CreatedCallback],
                      eventCallback: Option[EventCallback],
                      isRootOperator: Boolean,
                      _parentNode: ActorRef*
                     )
  extends UnaryNode {

  var parentNode: ActorRef = _parentNode.head
  val directory = if(new File("./logs").isDirectory) Some(new File("./logs")) else {
    logger.info("Invalid directory path")
    None
  }

  val csvWriter = directory map { directory => new PrintStream(new File(directory, s"filter-messages.csv"))
  } getOrElse java.lang.System.out
  csvWriter.println("time\tfiltered\tvalue1\tvalue2")

  override def childNodeReceive: Receive = super.childNodeReceive orElse {
    case event: Event =>
      val s = sender()
      if (parentsList.contains(s)) {
        if (query.cond(event))
          emitEvent(event)
      } else log.info(s"received event $event from $s, \n parentlist: \n $parentsList")

    case unhandledMessage => log.info(s"${self.path.name} received msg ${unhandledMessage.getClass} from ${sender()}")
  }

}

