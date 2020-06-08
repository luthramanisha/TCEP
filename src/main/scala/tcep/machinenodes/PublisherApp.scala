package tcep.machinenodes

import akka.actor.{ActorSystem, Address, Props}
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.DistVivaldiActor
import tcep.config.ConfigurationParser
import tcep.data.Events.Event1
import tcep.machinenodes.helper.actors.TaskManagerActor
import tcep.publishers.{RegularPublisher, UnregularPublisher}
import tcep.simulation.tcep.SimulationRunner.options
import tcep.simulation.tcep.{LinearRoadDataNew, MobilityData, YahooDataNew}

import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
import scala.sys.process._
import scala.util.Random

/**
  * Created by raheel
  * on 09/08/2017.
  * updated by Niels on 15/06/2018
  */
object PublisherApp extends ConfigurationParser with App {
  try {
    logger.info(s"booting up PublisherApp")
    logger.info("args: " + getArgs.toList.toString)

    Future {
      val res = "ntpd -s".!!
      logger.info(s"ntpd called $res")
    }

    val actorSystem: ActorSystem = ActorSystem(config.getString("clustering.cluster.name"), config)
    val cluster = Cluster(actorSystem)
    implicit val creatorAddress: Address = cluster.selfAddress
    DistVivaldiActor.createVivIfNotExists(actorSystem)
    // publishers can host operators
    actorSystem.actorOf(Props(classOf[TaskManagerActor]), "TaskManager")
    val publisherName = s"P:${options.getOrElse('ip, ipDefault)}:${options.getOrElse('port, 0)}"
    //val eventIntervalMicros = ConfigFactory.load().getInt("constants.event-interval-microseconds")
    val eventRate = 1000000 / options.getOrElse('eventRate, "1").toInt
    val logFilePathStr = System.getProperty("logFilePath").split("/")
    val idx = logFilePathStr.indexOf("logs")
    val traceLocation = logFilePathStr.slice(0, idx + 1).mkString("/") + "/../mobility_traces"

    options.getOrElse('kind, "none") match {

      case "SpeedPublisher" =>
        logger.info("starting to create SpeedPublisher")
        // publisher that emits speed values based on a tracefile generated from the madrid roadway traces
        // the tracefile for each publisher contains the speed values of multiple vehicles appended back to back,
        // i.e. when the first vehicle leaves the road, the trace continues with values from a vehicle entering at that time instant
        // -> the publisher is reused for that new vehicle
        // http://www.it.uc3m.es/madrid-traces/traces/ -> short_traces -> A6-d11-h08.dat
        type Time = Double
        val publisherID: Int = options.getOrElse('numberOfPublishers, "1").toInt
        val tracefile = s"$traceLocation/speedTrace$publisherID.csv"
        try {
          val sectionLength = ConfigFactory.load().getInt("constants.mobility-simulation.road-section-length")
          val mobilityTraces: HashMap[Time, MobilityData] = {
            var res = HashMap[Time, MobilityData]()
            try {
              val bufferedSource = Source.fromFile(tracefile)
              var headerSkipped = false
              for (line <- bufferedSource.getLines) {
                if (!headerSkipped) {
                  headerSkipped = true
                } else {
                  val cols = line.split(",").map(_.trim)
                  val sectionID = math.floor(cols(1).toDouble / sectionLength).toInt
                  res += (cols(0).toDouble -> MobilityData(sectionID, cols(3).toDouble))
                }
              }
              bufferedSource.close()
            } catch {
              case e: Throwable =>
                logger.error(s"could not find $tracefile, using placeholder data for events instead! " +
                  s"Not a problem as long as event data does not influence simulation results")
                res += 0.5 -> MobilityData(1, 0.0)
                res += 1.0 -> MobilityData(1, 0.0)
                res += 1.5 -> MobilityData(1, 0.0)
                res += 2.0 -> MobilityData(1, 0.0)
            }
            logger.info(s"loaded tracefile $tracefile with mobility traces: ${res.size} entries" +
              s" from ${res.keys.toList.min} to ${res.keys.toList.max}")
            res
          }

          //logger.debug(s"tracefile contents: \n ${mobilityTraces.toList.sortBy(_._1).map(e => s"\n${e._1} : ${e._2}")}")
          val pub = actorSystem.actorOf(Props(
            RegularPublisher(eventRate, id => {
              val key = (id.toDouble * 0.5d) % ((mobilityTraces.size + 1) / 2) // roll over after traces end
              if (mobilityTraces.contains(key)) Event1(mobilityTraces(key))
              else Event1(MobilityData(-1, 0.0d))
            })), publisherName)
          logger.info(s"creating SpeedPublisher number $publisherID $pub")

        } catch {
          case e: Throwable => logger.error(s"error while creating speed publisher with speed traces from file $tracefile", e)
        }

      case "DensityPublisher" =>

        val nSections = ConfigFactory.load().getInt("constants.number-of-road-sections")

        def getDensity(section: Int) = Random.nextInt(8) // TODO use values calculated from madrid traces
        val dps = 0 until nSections map { i =>
          actorSystem.actorOf(Props(RegularPublisher(eventRate, id => Event1(getDensity(i)))), publisherName + s"-densityPublisher-$i-")
        }
        logger.info(s"creating $nSections DensityPublishers with ActorRefs: \n ${dps.mkString("\n")}")

    case "LinearRoadPublisher" =>
      logger.info("starting to create LinearRoadPublisher")
      type Time = Int
      val pubId: Int = options.getOrElse('numberOfPublishers, "1").toInt
      val nextTime: Int = options.getOrElse('eventWaitTime, "30").toInt
      val tracefile = s"/app/mobility_traces/linearRoadT${nextTime}_P${pubId}.csv"
      try {
        val eventTrace: HashMap[Time, (LinearRoadDataNew, Double)] = {
          val bufferedSource = Source.fromFile(tracefile)
          var res = HashMap[Time, (LinearRoadDataNew, Double)]()
          var headerSkipped = false
          var eventId = 1
          for (line <- bufferedSource.getLines()) {
            if (!headerSkipped)
              headerSkipped = true
            else {
              val cols = line.split(",").map(_.trim)
              val id = cols(1).toInt
              val section = cols(3).toInt
              val speed = cols(2).toDouble
              val density = cols(4).toInt
              res += (eventId -> ((LinearRoadDataNew(id,section,density,speed), cols(5).toDouble)))
              eventId += 1
            }
          }
          bufferedSource.close()
          res
        }
        val pub = actorSystem.actorOf(Props(UnregularPublisher(90, id => {
          val key = id.toInt
          if (eventTrace.contains(key)) (Event1(eventTrace(key)._1), eventTrace(key)._2)
          else (Event1(LinearRoadDataNew(-1, -1, -1, -1)), 1)}
        )), publisherName)
      } catch {
        case e: Throwable => logger.error(s"error while creating linear road publisher from tracefile $tracefile", e)
      }

    case "YahooPublisher" =>
      logger.info("starting to create YahooPublisher")
      type Time = Int
      val pubId: Int = options.getOrElse('numberOfPublishers, "1").toInt
      val tracefile = s"/app/mobility_traces/yahooTraceP${pubId}.csv"
      val nextTime: Int = options.getOrElse('eventWaitTime, "30").toInt
      try {
        val eventTrace: HashMap[Time, (YahooDataNew, Double)] = {
          val bufferedSource = Source.fromFile(tracefile)
          var res = HashMap[Time, (YahooDataNew, Double)]()
          var headerSkipped = false
          var eventId = 1
          for (line <- bufferedSource.getLines()) {
            if (!headerSkipped)
              headerSkipped = true
            else {
              val cols = line.split(",").map(_.trim)
              val adId = cols(3).toInt
              val eventType = cols(5).toInt
              res += (eventId -> ((YahooDataNew(adId, eventType), cols(6).toDouble)))
              eventId += 1
            }
          }
          bufferedSource.close()
          res
        }
        val pub = actorSystem.actorOf(Props(UnregularPublisher(30, id => {
          val key = id.toInt
          if (eventTrace.contains(key)) (Event1(eventTrace(key)._1), eventTrace(key)._2)
          else (Event1(YahooDataNew(-1, -1)), 0.1)})), publisherName)
      } catch {
        case e: Throwable => logger.error(s"error while creating yahoo publisher from tracefile $tracefile", e)
      }

      case _ =>
        val pub = actorSystem.actorOf(Props(RegularPublisher(eventRate, id => Event1(id))), publisherName)
        logger.info(s"creating RegularPublisher with ActorRef: $pub")
    }
  } catch {
    case e: Throwable =>
      logger.error("error while starting PublisherApp: ", e)
  }

  def randomValue(lower: Int, upper: Int): Double = {
    if (lower <= upper)
      (upper - lower) * Random.nextDouble() + lower
    else lower.toDouble
  }

  override def getRole: String = "Publisher, Candidate"

  override def getArgs: Array[String] = args
}
