package tcep.machinenodes

import akka.actor.{ActorSystem, Props}
import org.discovery.vivaldi.DistVivaldiActor
import tcep.config.ConfigurationParser
import tcep.machinenodes.helper.actors.TaskManagerActor
import tcep.placement.vivaldi.VivaldiRefNode

/**
  * Just creates a `VivaldiRefNode` which is used by other nodes to set their coordinates
  * Created by raheel
  * on 09/08/2017.
  */
object VivaldiApp extends ConfigurationParser with App {
  logger.info("booting up Vivaldi's App")

  val actorSystem: ActorSystem = ActorSystem(config.getString("clustering.cluster.name"), config)
  DistVivaldiActor.createVivIfNotExists(actorSystem)
  actorSystem.actorOf(Props(classOf[VivaldiRefNode]), "VivaldiRef")
  actorSystem.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")


  override def getRole: String = "VivaldiRef"
  override def getArgs: Array[String] = args
}

