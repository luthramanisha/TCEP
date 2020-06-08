package tcep.placement.vivaldi

import java.util.concurrent.TimeUnit

import akka.actor.ActorLogging
import akka.pattern.pipe
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.DistVivaldiActor
import tcep.ClusterActor
import tcep.machinenodes.helper.actors.CoordinatesRequest


/**
  * Created by raheel
  * on 12/08/2017.
  *
  * landmark node with two purposes: 1. first cluster node 2. additional reference node to ping
  */

class VivaldiRefNode extends ClusterActor with ActorLogging {

  implicit val timeout = Timeout(ConfigFactory.load().getInt("constants.coordinate-request-timeout"), TimeUnit.SECONDS)

  override def preStart(): Unit = {
    super.preStart()
    log.info("booting vivaldi ref node")
  }

  override def receive: Receive = {

    case CoordinatesRequest(address) =>
      val s = sender()
      DistVivaldiActor.getCoordinates(cluster, address) pipeTo s
      s ! DistVivaldiActor.getCoordinates(cluster, address)

    case message => log.info(s"ignoring unknown message: $message")
  }


}
