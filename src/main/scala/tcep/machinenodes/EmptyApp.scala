package tcep.machinenodes

import akka.actor.{ActorSystem, Props}
import org.discovery.vivaldi.DistVivaldiActor
import tcep.config.ConfigurationParser
import tcep.machinenodes.helper.actors.TaskManagerActor

/**
  * Just creates a `TaskManagerActor` which could receive tasks from PlacementAlgorithms
  * Created by raheel
  * on 09/08/2017.
  */
object EmptyApp extends ConfigurationParser with App {
  logger.info("booting up EmptyApp")

  val actorSystem: ActorSystem = ActorSystem(config.getString("clustering.cluster.name"), config)
  DistVivaldiActor.createVivIfNotExists(actorSystem)
  val taskManager = actorSystem.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")

  override def getRole: String = "Candidate"
  override def getArgs: Array[String] = args
}