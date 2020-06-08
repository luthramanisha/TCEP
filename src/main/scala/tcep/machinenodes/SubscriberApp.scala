package tcep.machinenodes

import akka.actor.{ActorSystem, Props}
import tcep.config.ConfigurationParser
import tcep.machinenodes.helper.actors.TaskManagerActor

/**
  * Startup Subscriber Application and Creates TaskManagerActor
  *
  * @example sbt "runMain tcep.subscriber.SubscriberApp portNo"
  */

object SubscriberApp extends App with ConfigurationParser {
  logger.info("booting subscriber")

  Thread.sleep(30000)

  val actorSystem: ActorSystem = ActorSystem(config.getString("clustering.cluster.name") , config)
  actorSystem.actorOf(Props[Subscriber],"subscriber")

  actorSystem.actorOf(Props(new TaskManagerActor()), "TaskManager")

  override def getRole: String = "Subscriber"
  override def getArgs: Array[String] = args
}
