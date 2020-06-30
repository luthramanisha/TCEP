package tcep.config

import java.net.InetAddress

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/**
  * Created by anjaria on 31.07.17.
  */
trait ConfigurationParser {
  lazy val logger = LoggerFactory.getLogger(getClass)

  def getArgs: Array[String]

  lazy val argList = getArgs.toList
  type OptionMap = Map[Symbol, String]
  lazy val ipDefault = InetAddress.getLocalHost().getHostAddress()

  private def readArgs(map: OptionMap, list: List[String]): OptionMap = {
    logger.info(s"args: $list")
    list match {
      case Nil => map
      case "--ip" :: value :: tail =>
        readArgs(map ++ Map('ip -> value), tail)
      case "--port" :: value :: tail =>
        readArgs(map ++ Map('port -> value), tail)
      case "--kind" :: value :: tail =>
        readArgs(map ++ Map('kind -> value), tail)
      case "--dir" :: value :: tail =>
        readArgs(map ++ Map('dir -> value), tail)
      case "--mode" :: value :: tail =>
        readArgs(map ++ Map('mode -> value), tail)
      case "--duration" :: value :: tail =>
        readArgs(map ++ Map('duration -> value), tail)
      case "--initialAlgorithm" :: value :: tail =>
        readArgs(map ++ Map('initialAlgorithm -> value), tail)
      case "--baseLatency" :: value :: tail =>
        readArgs(map ++ Map('baseLatency -> value), tail)
      case "--numberOfPublishers" :: value :: tail =>
        readArgs(map ++ Map('numberOfPublishers -> value), tail)
      case "--maxPubToClientHops" :: value :: tail =>
        readArgs(map ++ Map('maxPubToClientHops -> value), tail)
      case "--query" :: value :: tail =>      //specify query to be evaluated
        readArgs(map ++ Map('query -> value), tail)
      case "--mapek" :: value :: tail =>
        readArgs(map ++ Map('mapek -> value), tail)
      case "--req" :: value :: tail =>
        readArgs(map ++ Map('req -> value), tail)
      case "--eventRate" :: value :: tail =>
        readArgs(map ++ Map('eventRate -> value), tail)
      case "--transitionStrategy" :: value :: tail =>
        readArgs(map ++ Map('transitionStrategy -> value), tail)
      case "--transitionExecutionMode" :: value :: tail =>
        readArgs(map ++ Map('transitionExecutionMode -> value), tail)
      case "--loadTestMax" :: value :: tail =>
        readArgs(map ++ Map('loadTestMax -> value), tail)
      case option :: tail =>
        logger.error(s"Unknown option $option $tail");
        readArgs(map, tail)
    }
  }

  lazy val options = readArgs(Map(), argList)
  lazy val config = ConfigFactory.parseString(s"akka.remote.artery.canonical.port=${options('port)}")
      .withFallback(ConfigFactory.parseString(s"akka.remote.artery.canonical.hostname=${options.getOrElse('ip, ipDefault)}"))
      .withFallback(ConfigFactory.parseString(s"akka.remote.classic.netty.tcp.port=${options('port)}"))
      .withFallback(ConfigFactory.parseString(s"akka.remote.classic.netty.tcp.hostname=${options.getOrElse('ip, ipDefault)}"))
      .withFallback(ConfigFactory.parseString(s"akka.cluster.roles=[$getRole]"))
      .withFallback(ConfigFactory.load())

  def getRole: String
}
