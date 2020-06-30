package tcep.machinenodes

import akka.actor.{ActorSystem, Props}
import org.discovery.vivaldi.DistVivaldiActor
import tcep.config.ConfigurationParser
import tcep.machinenodes.consumers.{AccidentConsumer, AnalysisConsumer, AvgSpeedConsumer, TollComputingConsumer}

/**
  * Startup Subscriber Application and Creates TaskManagerActor
  *
  * @example sbt "runMain tcep.subscriber.SubscriberApp portNo"
  */

object ConsumerApp extends ConfigurationParser with App {
  logger.info(s"booting subscriber")
  logger.info(s"args: " + getArgs.toList.toString)

  //Thread.sleep(30000)

  val actorSystem: ActorSystem = ActorSystem(config.getString("clustering.cluster.name") , config)
  DistVivaldiActor.createVivIfNotExists(actorSystem)
  logger.info("Adding Consumer")
  //actorSystem.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
  val loadTestMax = options.getOrElse('loadTestMax, "1").toInt
  0 until loadTestMax map { i =>
    val consumer = options.getOrElse('kind, "none") match {
      case "AvgSpeed" => actorSystem.actorOf(Props[AvgSpeedConsumer], s"consumer$i")
      case "Accident" => actorSystem.actorOf(Props[AccidentConsumer], s"consumer$i")
      case "Toll" => actorSystem.actorOf(Props[TollComputingConsumer], s"consumer$i")
      case "AdAnalysis" => actorSystem.actorOf(Props[AnalysisConsumer], s"consumer$i")
    }
    logger.info(s"started consumer actor $consumer")
  }

  override def getRole: String = "Consumer"
  //override def getRole: String = "Subscriber"
  override def getArgs: Array[String] = args
  logger.info(s"Received options $options")
}
