package tcep.machinenodes

import akka.actor.{ActorSystem, Props}
import org.discovery.vivaldi.DistVivaldiActor
import tcep.config.ConfigurationParser
import tcep.machinenodes.helper.actors.MasterNode

/**
  * Just creates a `TaskManagerActor` which could receive tasks from PlacementAlgorithms
  * Created by raheel
  * on 09/08/2017.
  */
object MasterApp extends ConfigurationParser with App {
  logger.info("booting up EmptyApp")

  Thread.sleep(30000)
  val actorSystem: ActorSystem = ActorSystem(config.getString("clustering.cluster.name"), config)
  DistVivaldiActor.createVivIfNotExists(actorSystem)
  actorSystem.actorOf(Props(classOf[MasterNode]), "MasterNodeManager")

  override def getRole: String = "Candidate"
  override def getArgs: Array[String] = args
}