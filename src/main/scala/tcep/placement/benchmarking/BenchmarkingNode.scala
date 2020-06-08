package tcep.placement.benchmarking

import java.util.concurrent.TimeUnit

import akka.actor.ActorLogging
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import tcep.data.Queries.{LatencyRequirement, LoadRequirement, MessageHopsRequirement, Requirement}
import tcep.placement.PlacementStrategy
import tcep.placement.vivaldi.VivaldiCoordinates

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime._

/**
  * Loads benchmarking configuration from conf file
  */
class BenchmarkingNode extends VivaldiCoordinates with ActorLogging {
  implicit val resolveTimeout = Timeout(40, TimeUnit.SECONDS)

  override def preStart(): Unit = {
    super.preStart()
    log.info("booting BenchmarkingNode")
  }

  override def receive: Receive = {
    case SelectPlacementAlgorithm(constraints, requirements) => sender() ! BenchmarkingNode.selectBestPlacementAlgorithm(constraints, requirements)
    case message => log.info(s"ignoring unknown message: $message")
  }

  override def postStop(): Unit = {
    super.postStop()
  }
}

object BenchmarkingNode {
  val algorithms = new ListBuffer[PlacementAlgorithm]()
  lazy val log = LoggerFactory.getLogger(getClass)
  var initialized = false
  parseConfiguration()

  private def getConfiguration: Config = ConfigFactory.load()

  def parseConfiguration() = {
    val algorithmNames = getConfiguration.getConfig("benchmark.general").getStringList("algorithms")

    algorithmNames.forEach(algo => {
      val algoConf = getConfiguration.getConfig("benchmark").getConfig(algo)
      val optimizations: List[String] = algoConf.getStringList("optimizationCriteria").toList
      val networkConstraints: List[NetworkChurnRate] = algoConf.getStringList("constraints").toList
        .map {
          case "HighChurnRate" => HighChurnRate
          case "LowChurnRate" => LowChurnRate
        }

      val strategy = getObjectInstance[PlacementStrategy](algoConf.getString("class"))
      algorithms += PlacementAlgorithm(strategy, networkConstraints, optimizations, algoConf.getInt("score"))
    })

    initialized = true
  }

  def selectBestPlacementAlgorithm(constraints: List[NetworkChurnRate], requirements: List[Requirement]): PlacementAlgorithm = {
    if(!initialized){
      parseConfiguration()
    }

    var availableAlgos = algorithms.toList
    log.info(s"selecting best algorithm for requirements $requirements and network condition $constraints among \n ${availableAlgos.mkString("\n")}")
    for (requirement <- requirements) { filterAlgos(requirement.name) }

    def filterAlgos(requirement: String) = {
      availableAlgos = availableAlgos.filter(p => p.demands.contains(requirement))
    }

    log.info(s"algorithms meeting requirements and constraints: \n ${availableAlgos.mkString("\n")}")
    if (availableAlgos.length >= 2) {
      val filtered = availableAlgos.filter(a => constraints.toSet.subsetOf(a.contraints.toSet))
      log.info(s"selecting algorithm with lowest score among ${filtered}")
      filtered.sortBy(_.score).head
    } else {
      if(availableAlgos.isEmpty){
        log.info(s"can't fulfill network constraints or requirements ${constraints} ${requirements}, using ${algorithms.head}")
        algorithms.head
      }else{
        log.info(s"selecting ${availableAlgos.head}")
        availableAlgos.head
      }
    }
  }

  def getObjectInstance[A](clsName: String): A = {
    val ms = currentMirror staticModule clsName
    val moduleMirror = currentMirror reflectModule ms
    moduleMirror.instance.asInstanceOf[A]
  }

  def getPlacementAlgorithms(): List[PlacementAlgorithm] = {
    if (!initialized)
      parseConfiguration()
    this.algorithms.toList
  }
}

case class PlacementAlgorithm(placement: PlacementStrategy, contraints: List[NetworkChurnRate],
                              demands: List[String], score: Int) {

  def containsDemand(demand: Requirement): Boolean = {
    demand match {
      case l: LatencyRequirement => demands.contains(l.name)
      case l: LoadRequirement => demands.contains(l.name)
      case m: MessageHopsRequirement => demands.contains(m.name)
      case _ => false //ignoring unsupported requirement
      //Include message output rate
    }
  }
}

//DTOs
case class SelectPlacementAlgorithm(networkContraints: List[NetworkChurnRate], requirements: List[Requirement])
