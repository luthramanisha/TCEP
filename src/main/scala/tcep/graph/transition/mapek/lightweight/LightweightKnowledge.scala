package tcep.graph.transition.mapek.lightweight

import akka.actor.ActorRef
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import tcep.data.Queries
import tcep.data.Queries._
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.transition.KnowledgeComponent
import tcep.graph.transition.MAPEK.GetPlacementStrategyName
import tcep.graph.transition.mapek.lightweight.LightweightKnowledge.{GetLogData, GetTransitionData, UpdatePerformance}
import tcep.graph.transition.mapek.lightweight.LightweightMAPEK.GetConsumer
import tcep.placement.PlacementStrategy
import tcep.placement.benchmarking.{BenchmarkingNode, PlacementAlgorithm}
import tcep.utils.SpecialStats

import scala.collection.JavaConverters._

class LightweightKnowledge(mapek: LightweightMAPEK, query: Query, transitionConfig: TransitionConfig/*, allRecords: AllRecords*/, currentPlacementStrategy: PlacementStrategy, consumer: ActorRef)
  //extends KnowledgeComponent(query, mode, currentPlacementStrategy = BenchmarkingNode.selectBestPlacementAlgorithm(List(), Queries.pullRequirements(query, List()).toList).placement) {
  extends KnowledgeComponent(query, transitionConfig, currentPlacementStrategy) {

  var placementAlgorithms: List[PlacementAlgorithm] = List()
  var fitnessScores: FitnessContainer = new FitnessContainer()
  var fitnessLogData: (List[Any], List[Any]) = (List.empty[Any], List.empty[Any])

  override def preStart(): Unit = {
    super.preStart()
    log.info("Starting LightweightKnowledge")
    placementAlgorithms = BenchmarkingNode.getPlacementAlgorithms()
    if(ConfigFactory.load().getBoolean("constants.mapek.only-suitable")){
      val requirements = Queries.pullRequirements(query, List()).toList
      var availableAlgos = placementAlgorithms
      for (req <- requirements) {
        filterAlgos(req.name)
      }
      def filterAlgos(requirement: String) ={
        availableAlgos = availableAlgos.filter(p => p.demands.contains(requirement))
      }
      placementAlgorithms = availableAlgos
    }
    log.info(s"all algorithms: $placementAlgorithms")
    for (algo <- ConfigFactory.load().getStringList("constants.mapek.blacklisted-algorithms").asScala.toList) {
      placementAlgorithms = placementAlgorithms.filter(p => "fs" + p.placement.name != algo)
    }
    log.info(s"available algorithms: ${placementAlgorithms}")
    //placementAlgorithms = placementAlgorithms.filter(p => p.placement.name != "Random")
    //placementAlgorithms = placementAlgorithms.filter(p => p.placement.name != "Rizou")
    //placementAlgorithms = placementAlgorithms.filter(p => p.placement.name != "ProducerConsumer")
    //algorithmsToInit = placementAlgorithms.slice(1, placementAlgorithms.size)

    for(algo <- placementAlgorithms)
      fitnessScores.addAlgorithm(algo.placement.name)
    for(req <- requirements)
      fitnessScores.addRequirement(req.name)

    log.info(s"lightweight knowledge requirements: $requirements")
  }

  override def postStop(): Unit = {
    super.postStop()
    fitnessScores.stop()
  }

  override def receive: Receive = super.receive orElse {

    case GetLogData =>
      //this.fitnessScores.updateFitness()
      //sender() ! this.fitnessScores.getLogData()
      sender() ! this.fitnessLogData

    case UpdatePerformance(performanceMeasures) =>
      log.info("Update Performance received")
      this.updateMeasurements(performanceMeasures)
      //this.fitnessScores.receive(this.requirements.toList, performanceMeasures, this.currentPlacementStrategy.name)
      /*for {
        //deployComplete <- (this.self ? IsDeploymentComplete).mapTo[Boolean]
        //transitionStatus <- (this.self ? GetTransitionStatus).mapTo[Int]
        allRecords <- (this.consumer ? GetAllRecords).mapTo[AllRecords]
        //if deployComplete && transitionStatus == 0 && allRecords.allDefined
        if allRecords.allDefined
        currentPlacement <- (this.self ? GetPlacementStrategyName).mapTo[String]
        req <- (this.self ? GetRequirements).mapTo[List[Requirement]]
      } yield {
        //val req = this.requirements

        log.info(s"Current Requirements are $req and placement is $currentPlacement")
        if (this.deploymentComplete && this.transitionStatus == 0)
          this.fitnessScores.receive(req, performanceMeasures, currentPlacement)
        /*if (this.deploymentComplete & this.transitionStatus == 0) {
          for (measure <- performanceMeasures) {
            val requirement = req.find(p => p.name == measure._1)
            requirement.get match {
              case latencyReq: LatencyRequirement =>
                val reqValue = latencyReq.latency.toMillis
                val currentValue = measure._2
                log.info(s"Requirement is: $reqValue and current Value is: $currentValue")
                if (compareHelper(reqValue, latencyReq.operator, currentValue))
                  fitnessScores.increaseFitness(currentPlacement, requirement.get.name)
                else
                  fitnessScores.addSample(currentPlacement, requirement.get.name)
              case freqReq: FrequencyRequirement =>
                val reqValue = freqReq.frequency.frequency.asInstanceOf[Double]
                val currentValue = measure._2
                log.info(s"Requirement is: $reqValue and current Value is: $currentValue")
                if (compareHelper(reqValue, freqReq.operator, currentValue))
                  fitnessScores.increaseFitness(currentPlacement, requirement.get.name)
                else
                  fitnessScores.addSample(currentPlacement, requirement.get.name)
              case loadReq: LoadRequirement =>
                val reqValue = loadReq.machineLoad.value
                val currentValue = measure._2
                log.info(s"Requirement is: $reqValue and current Value is: $currentValue")
                if (compareHelper(reqValue, loadReq.operator, currentValue))
                  fitnessScores.increaseFitness(currentPlacement, requirement.get.name)
                else
                  fitnessScores.addSample(currentPlacement, requirement.get.name)
              case msgHopReq: MessageHopsRequirement =>
                val reqValue = msgHopReq.requirement.asInstanceOf[Double]
                val currentValue = measure._2
                log.info(s"Requirement is: $reqValue and current Value is: $currentValue")
                if (compareHelper(reqValue, msgHopReq.operator, currentValue))
                  fitnessScores.increaseFitness(currentPlacement, requirement.get.name)
                else
                  fitnessScores.addSample(currentPlacement, requirement.get.name)
            }
          }
        }*/
      }
      //fitnessScores.updateFitness()*/

    case GetTransitionData =>
      sender() ! fitnessScores.samplePlacement()
      this.fitnessLogData = fitnessScores.getLogData()

    case GetConsumer =>
      sender() ! this.consumer
    }

  def updateMeasurements(measures: List[(String, Double)]) = {
    for {
      currentStrategy <- (this.self ? GetPlacementStrategyName).mapTo[String]
    } yield {
      val updateStartMillis = System.currentTimeMillis()
      val updateStartNanos = System.nanoTime()
      this.fitnessScores.receive(this.requirements.toList, measures, currentStrategy)
      val nanos = System.nanoTime() - updateStartNanos
      val millis = System.currentTimeMillis() - updateStartMillis
      SpecialStats.log(this.getClass.toString, "LightweightUpdateTimings", s"$nanos;ns;$millis;ms")
    }
  }

  /**
    * Created by Niels on 14.04.2018.
    * Copied from RequirementChecker
    *
    * helper function to compare a requirement value to an actual value
    *
    * @param reqVal value of the requirement
    * @param op comparison operator
    * @param otherVal value to compare to
    * @return true if requirement is condition holds, false if violated
    */
  def compareHelper(reqVal: Double, op: Operator, otherVal: Double): Boolean = {
    op match {
      case Equal => reqVal == otherVal
      case NotEqual => reqVal != otherVal
      case Greater => otherVal > reqVal
      case GreaterEqual => otherVal >= reqVal
      case Smaller => otherVal < reqVal
      case SmallerEqual => otherVal <= reqVal
    }
  }
}

object LightweightKnowledge{

  case class UpdatePerformance(performanceMeasures: List[(String, Double)])
  case class GetTransitionData()
  case class GetLogData()
}