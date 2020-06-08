package tcep.machinenodes.helper.actors

import akka.actor.{ActorLogging, Address}
import akka.cluster.Member
import com.typesafe.config.ConfigFactory
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.placement.HostInfo
import tcep.placement.vivaldi.VivaldiCoordinates
import tcep.simulation.adaptive.cep.SystemLoad

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

/**
  * Created by raheel
  * on 09/08/2017.
  */

class MasterNode extends VivaldiCoordinates with ActorLogging {

  val networkNodes = ConfigFactory.load().getStringList("master-nodes")
  val upNodes: ListBuffer[Address] = ListBuffer[Address]()


  def updateOtherMasterNodes(): Unit = Future {

    for (node <- networkNodes) {
      val remoteActor = context.actorSelection(s"akka.tcp://RemoteSystem@$node/user/MasterNodeManager")
      remoteActor ! MasterNodeUp(cluster.selfAddress)
    }
  }

  override def preStart(): Unit = {
    super.preStart()
    log.info("booting up MasterNodeManager")
    updateOtherMasterNodes()
  }

  override def receive: Receive = super.receive orElse {
    case MasterNodeUp(upNode) => {
      upNodes += upNode
    }
    /*
  case p: PietzuchOperator => {
    val requester = sender()
    log.info(s"received pietzuch placement task")
    Future {
      requester ! PietzuchAlgorithm.findOptimalNode(this.context, cluster, p.dependencies, p.askerInfo, p.askerInfo.operator)
    }
  }*/
    case LoadRequest() => {
      sender() ! SystemLoad.getSystemLoad
    }
    case _ => log.info("ignoring unknown task")
  }

  override def memberUp(member: Member): Unit = {
    super.memberUp(member)
    //TODO: take system name from conf i.e. tcep
    upNodes.foreach(node => context.actorSelection(s"akka.tcp://tcep@${node.host.get}:${node.port.get}/user/MasterNodeManager"))
  }
}

class MasterNodeActions()
case class MasterNodeUp(address: Address) extends MasterNode
case class MemberUp(member: Member) extends MasterNode
case class PietzuchOperator(dependencies: Dependencies, askerInfo: HostInfo)