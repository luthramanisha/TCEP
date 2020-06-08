package tcep.placement.vivaldi

import java.util.concurrent.TimeUnit

import akka.actor.ActorLogging
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberUp}
import akka.cluster.Member
import akka.pattern.pipe
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.DistVivaldiActor
import tcep.ClusterActor
import tcep.machinenodes.helper.actors.CoordinatesRequest

/**
  * Created by raheel
  * on 04/08/2017.
  */

/**
  * Every class which needs coordinates, will have to inherit this class
  */

trait VivaldiCoordinates extends ClusterActor with ActorLogging {

  val coordinateTimeout = Timeout(ConfigFactory.load().getInt("constants.coordinate-request-timeout"), TimeUnit.SECONDS)

  def currentClusterState(state: CurrentClusterState): Unit = {}

  def memberUp(member: Member): Unit = {}

  override def preStart(): Unit = {
    super.preStart()
    DistVivaldiActor.createVivIfNotExists(this.context.system)
  }

  override def receive: Receive = {
    case MemberUp(member) => memberUp(member)
    case state: CurrentClusterState => currentClusterState(state)

    case CoordinatesRequest(address) =>
      val s = sender()
      implicit val timeout = coordinateTimeout
      DistVivaldiActor.getCoordinates(cluster, address) pipeTo s

    case _: MemberEvent => // ignore
  }

}
