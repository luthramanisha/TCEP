package tcep.graph.transition.mapek.lightweight

import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import tcep.data.Queries._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

class FitnessContainer {

  private val log: Logger = LoggerFactory.getLogger(getClass)
  var possibleAlgorithms: ListBuffer[String] = new ListBuffer()
  var requirements: ListBuffer[String] = new ListBuffer()
  var scores: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]] = mutable.HashMap.empty[String, mutable.HashMap[String,mutable.HashMap[String,Double]]]
  var decay: Double = ConfigFactory.load().getDouble("constants.mapek.lightweight-decay")

  def addAlgorithm(name: String): Unit = {
    if (!possibleAlgorithms.contains(name)) {
      possibleAlgorithms += name
      for (req <- requirements) {
        scores += (req -> mutable.HashMap(name -> mutable.HashMap("scored" -> 0.0)))
        scores.get(req).get(name) += ("total" -> 0.0)
        scores.get(req).get(name) += ("fitness" -> 0.0)
      }
    }
  }

  def addAlgorithms(names: List[String]): Unit = {
    for (name <- names)
      addAlgorithm(name)
  }

  def addRequirement(name: String): Unit = {
    if (!requirements.contains(name)){
      requirements += name
      scores += (name -> mutable.HashMap.empty[String, mutable.HashMap[String, Double]])
      for (op <- possibleAlgorithms) {
        scores.get(name).get += (op -> mutable.HashMap("scored" -> 0.0))
        scores.get(name).get(op) += ("total" -> 0.0)
        scores.get(name).get(op) += ("fitness" -> 0.0)
      }
    }
  }

  def addRequirements(names: List[String]): Unit = {
    for (name <- names)
      addRequirement(name)
  }

  def increaseFitness(op: String, req: String): Unit = {
    var newVal = scores.get(req).get(op).get("scored").get + 1.0
    if (!requirements.contains(req))
      addRequirement(req)
    scores.get(req).get(op) += ("scored" -> newVal)
    newVal = scores.get(req).get(op).get("total").get + 1.0
    scores.get(req).get(op) += ("total" -> newVal)
  }

  def addSample(op: String, req: String): Unit = {
    if (!requirements.contains(req))
      addRequirement(req)
    val newVal = scores.get(req).get(op).get("total").get + 1.0
    scores.get(req).get(op) += ("total" -> newVal)
  }

  def getOpScore(qos: String, op: String): Double ={
    val score = scores.get(qos).get(op).get("scored").get/(if (scores.get(qos).get(op).get("total").get > 0.0) scores.get(qos).get(op).get("total").get else 1.0)
    score
  }

  def receive(req: List[Requirement], performanceMeasures: List[(String,Double)], currentPlacement: String) = {

    for (measure <- performanceMeasures) {
      val requirement = req.find(p => p.name == measure._1)
      if (requirement.isDefined) {
        requirement.get match {
          case latencyReq: LatencyRequirement =>
            val reqValue = latencyReq.latency.toMillis
            val currentValue = measure._2
            log.info(s"Requirement is: $reqValue and current Value is: $currentValue")
            if (compareHelper(reqValue, latencyReq.operator, currentValue))
              this.increaseFitness(currentPlacement, requirement.get.name)
            else
              this.addSample(currentPlacement, requirement.get.name)
          case freqReq: FrequencyRequirement =>
            val reqValue = freqReq.frequency.frequency.asInstanceOf[Double]
            val currentValue = measure._2
            log.info(s"Requirement is: $reqValue and current Value is: $currentValue")
            if (compareHelper(reqValue, freqReq.operator, currentValue))
              this.increaseFitness(currentPlacement, requirement.get.name)
            else
              this.addSample(currentPlacement, requirement.get.name)
          case loadReq: LoadRequirement =>
            val reqValue = loadReq.machineLoad.value
            val currentValue = measure._2
            log.info(s"Requirement is: $reqValue and current Value is: $currentValue")
            if (compareHelper(reqValue, loadReq.operator, currentValue))
              this.increaseFitness(currentPlacement, requirement.get.name)
            else
              this.addSample(currentPlacement, requirement.get.name)
          case msgHopReq: MessageHopsRequirement =>
            val reqValue = msgHopReq.requirement.asInstanceOf[Double]
            val currentValue = measure._2
            log.info(s"Requirement is: $reqValue and current Value is: $currentValue")
            if (compareHelper(reqValue, msgHopReq.operator, currentValue))
              this.increaseFitness(currentPlacement, requirement.get.name)
            else
              this.addSample(currentPlacement, requirement.get.name)
        }
      }
    }
    this.updateFitness()
  }

  def updateFitness(): Unit = {
    var scoreSave = mutable.HashMap.empty[String, mutable.HashMap[String, Double]]
    for (qos <- scores) {
      log.info(s"for ${qos._1}")
      scoreSave += (qos._1 -> mutable.HashMap("mean" -> 0.0))
      scoreSave.get(qos._1).get += ("max" -> 0.0)
      scoreSave.get(qos._1).get += ("min" -> Double.MaxValue)
      for (op <- qos._2){
        val currentScore = getOpScore(qos._1, op._1)
        val newMean = currentScore + scoreSave.get(qos._1).get("mean")
        scoreSave.get(qos._1).get += ("mean" -> newMean)
        if (scoreSave.get(qos._1).get("min") > currentScore)
          scoreSave.get(qos._1).get += ("min" -> currentScore)
        if (scoreSave.get(qos._1).get("max") < currentScore)
          scoreSave.get(qos._1).get += ("max" -> currentScore)
      }
      scoreSave.get(qos._1).get += ("mean" -> scoreSave.get(qos._1).get("mean") / possibleAlgorithms.length)
      val range = if (scoreSave.get(qos._1).get("max")-scoreSave.get(qos._1).get("min") > 0) scoreSave.get(qos._1).get("max")-scoreSave.get(qos._1).get("min") else 1.0
      log.info(s"Scoresave is: $scoreSave and range is $range")
      for (op <- qos._2) {
        var scoreComputation = getOpScore(qos._1, op._1) - scoreSave.get(qos._1).get("mean")
        scoreComputation = scoreComputation/range + 1.0
        if (scores.get(qos._1).get(op._1).contains("fitness")) {
          val old = scores.get(qos._1).get(op._1).get("fitness").get
          val newScore = (old*decay)+((1-decay) * scoreComputation)
          scores.get(qos._1).get(op._1) += ("fitness" -> newScore)
        } else
          scores.get(qos._1).get(op._1) += ("fitness" -> scoreComputation)
      }
    }
    log.info(s"Fitness updated: $scores")
  }

  def getPlacementToInit(): String = {
    val req = scores.headOption
    var out = "None"
    for (op <- Random.shuffle(possibleAlgorithms)) {
      if (req.isDefined) {
        if(scores.get(req.get._1).get(op).get("total").get == 0.0)
          out = op
      } else {
        log.info(s"no requirements defined, selecting algorithm randomly among $possibleAlgorithms")
        out = op
      }
    }
    out
  }

  def samplePlacement(): String = {
    val toInit: String = getPlacementToInit()
    if (toInit.equals("None")) {
      log.info("Computing Fitness Scores")
      updateFitness()
      var opScores: mutable.HashMap[String, Double] = mutable.HashMap.empty[String, Double]
      for (qos <- scores) {
        for (op <- qos._2) {
          if (opScores.contains(op._1)) {
            val newVal = opScores.get(op._1).get + this.scores.get(qos._1).get(op._1).get("fitness").get
            opScores += (op._1 -> newVal)
          } else {
            opScores += (op._1 -> this.scores.get(qos._1).get(op._1).get("fitness").get)
          }
        }
      }
      val ranking = mutable.LinkedHashMap(opScores.toSeq.sortBy(_._2): _*)
      val probas = computeRankingProbabilities()
      val selection = selectAccordingToProbas(probas)
      log.info(s"Ranking is ${ranking.keys.toList}. Probabilities are: ${probas.toList.toString()}. Selection is: $selection")
      var rank = 0
      for (op <- ranking.toList) {
        /*plotOut.append(
          s"${op._1};${op._2};${probas.toList(rank)}"
        )
        plotOut.println()*/
        rank += 1
      }
      ranking.toList(selection)._1
    } else {
      toInit
    }
  }

  def getMinMaxProba(): (Double,Double) = {
    val fullScores: mutable.HashMap[String, Double] = mutable.HashMap.empty[String, Double]
    for (qos <- scores) {
      for (op <- scores.get(qos._1).get) {
        if (!fullScores.contains(op._1))
          fullScores += (op._1 -> 0.0)
        val old = fullScores.get(op._1)
        val newVal = scores.get(qos._1).get(op._1).get("fitness").get+old.get
        fullScores += (op._1 -> newVal)
      }
    }
    var sum = 0.0
    for (op <- fullScores)
      sum += fullScores.get(op._1).get
    var max = 0.0
    var min = Double.MaxValue
    for (op <- fullScores) {
      val tmp = fullScores.get(op._1).get / sum
      if (tmp > max)
        max = tmp
      if (tmp < min)
        min = tmp
    }
    (min, max)
  }

  def computeRankingProbabilities(): Array[Double] = {
    val N: Int = this.possibleAlgorithms.size
    val rankProbas: Array[Double] = new Array[Double](N)
    //val max: Double = ConfigFactory.load().getDouble("constants.mapek.lightweight-bestProbability")
    //val min: Double = 2-max
    val minMax = getMinMaxProba()
    //val max = minMax._2
    val min = minMax._1
    val max: Double = 2-min
    var tmp = 0.0
    for (i <- (0 to N-1)) {
      tmp = min+(max-min)*(i.toDouble/(N-1))
      //log.info(s"min is: $min; Range is: ${max-min}; i/(N-1) is ${i.toDouble/(N-1)}")
      tmp = tmp / N
      //log.info(s"Computed Probability for rank $i is $tmp")
      rankProbas(i) = tmp
    }
    rankProbas
  }

  def getLogData(): (List[Any],List[Any]) = {
    val score_out = new ListBuffer[Any]
    val proba_out = new ListBuffer[Any]
    val toInit = getPlacementToInit()
    var opScores: mutable.HashMap[String, Double] = mutable.HashMap.empty[String, Double]
    for (qos <- scores) {
      for (op <- qos._2) {
        if (opScores.contains(op._1)) {
          val newVal = opScores.get(op._1).get + this.scores.get(qos._1).get(op._1).get("fitness").get
          opScores += (op._1 -> newVal)
        } else {
          opScores += (op._1 -> this.scores.get(qos._1).get(op._1).get("fitness").get)
        }
      }
    }
    for (score <- opScores)
      score_out.append(score)
    if (toInit.equals("None")) {
      for (p <- computeRankingProbabilities())
        proba_out.append(p)
    } else {
      for (op <- possibleAlgorithms)
        proba_out.append(0.0)
    }
    (score_out.toList,proba_out.toList)
  }

  def selectAccordingToProbas(rankProbas: Array[Double]): Int = {
    val rnd: Double = scala.util.Random.nextDouble()
    var sum: Double = 0
    var out: Int = rankProbas.size-1
    breakable {
      for (i <- 0 to rankProbas.size-1) {
        sum += rankProbas(i)
        if (rnd <= sum){
          out = i
          break
        }
      }
    }
    out
  }

  def stop(): Unit = {
    //plotOut.close()
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
