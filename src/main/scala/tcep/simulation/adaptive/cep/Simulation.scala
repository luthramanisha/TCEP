package tcep.simulation.adaptive.cep

import tcep.simulation.adaptive.cep.Simulation._
import tcep.system.{Host, Operator, System}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.Random

object Simulation {
  case class HostId(id: Int) extends Host

  case class HostProps(
      latency: Seq[(Host, ContinuousBoundedValue[Duration])],
      bandwidth: Seq[(Host, ContinuousBoundedValue[Double])]) {
    def advance = HostProps(
      latency map { case (host, latency) => (host, latency.advance) },
      bandwidth map { case (host, bandwidth) => (host, bandwidth.advance) })
  }

  sealed trait Optimizing
  case object Maximizing extends Optimizing
  case object Minimizing extends Optimizing
}

class Simulation(system: System) {
  val random = new Random(0)
  val nodeCount = 25
  val stepDuration = 1.second

  object latency {
    implicit val addDuration: (Duration, Duration) => Duration = _ + _

    val template = ContinuousBoundedValue[Duration](
      Duration.Undefined,
      min = 2.millis, max = 100.millis,
      () => (0.4.millis - 0.8.milli * random.nextDouble, 1 + random.nextInt(900)))

    def apply() =
      template copy (value = 2.milli + 98.millis * random.nextDouble)
  }

  object bandwidth {
    implicit val addDouble: (Double, Double) => Double = _ + _

    val template = ContinuousBoundedValue[Double](
      0,
      min = 5, max = 100,
      () => (0.4 - 0.8 * random.nextDouble, 1 + random.nextInt(900)))

    def apply() =
      template copy (value = 5 + 95* random.nextDouble)
  }


  private def latencySelector(props: HostProps, host: Host): Duration =
    (props.latency collectFirst { case (`host`, latency) => latency }).get.value

  private def bandwidthSelector(props: HostProps, host: Host): Double =
    (props.bandwidth collectFirst { case (`host`, bandwidth) => bandwidth }).get.value

  private def latencyBandwidthSelector(props: HostProps, host: Host): (Duration, Double) =
    ((props.latency collectFirst { case (`host`, latency) => latency }).get.value,
     (props.bandwidth collectFirst { case (`host`, bandwidth) => bandwidth }).get.value)

  private def avg(durations: Seq[Duration]): Duration =
    if (durations.isEmpty)
      Duration.Zero
    else
      durations.foldLeft[Duration](Duration.Zero) { _ + _ } / durations.size

  private def avg(numerics: Seq[Double]): Double =
    if (numerics.isEmpty)
      0.0
    else
      numerics.sum / numerics.size

  private var hostProps: Map[Host, HostProps] = {
    val hostIds  = 0 until nodeCount map HostId
    (hostIds map { id =>
      id -> HostProps(
        hostIds collect { case otherId if otherId != id => otherId -> latency() },
        hostIds collect { case otherId if otherId != id => otherId -> bandwidth() })
    }).toMap
  }

  private var time = Duration.Zero


  def currentTime = time

  def advance(): Unit = {
    hostProps = (hostProps mapValues { _.advance }).view.force
    time += stepDuration
  }

  def measureLatency: Duration =
    measure(latencySelector, Minimizing, Duration.Zero) { _ + _ } { avg } { _.host }

  def measureBandwidth: Double =
    measure(bandwidthSelector, Maximizing, Double.MaxValue) { math.min } { avg } { _.host }

  private def measure[T: Ordering](
      selector: (HostProps, Host) => T,
      optimizing: Optimizing,
      zero: T)(
      merge: (T, T) => T)(
      avg: Seq[T] => T)(
      host: Operator => Host): T = {
    def measure(operator: Operator): T =
      if (operator.dependencies.isEmpty)
        zero
      else
        minmax(optimizing, operator.dependencies map { dependentOperator =>
          merge(measure(dependentOperator), selector(hostProps(host(operator)), host(dependentOperator)))
        })

    avg(system.consumers map measure)
  }

  def placeSequentially(): Unit = {
    def allOperators(operator: Operator): Seq[Operator] =
      operator +: (operator.dependencies flatMap allOperators)

    var nodeIndex = 0

    def placeOperator(operator: Operator): Unit = {
      if (nodeIndex >= nodeCount)
        throw new UnsupportedOperationException("not enough hosts")

      system place (operator, HostId(nodeIndex))
      nodeIndex += 1

      operator.dependencies foreach placeOperator
    }

    system.consumers foreach placeOperator
  }

  def placeOptimizingLatency(): Unit = {
    val measureLatency = measure(latencySelector, Minimizing, Duration.Zero) { _ + _ } { avg } _

    val placementsA = placeOptimizingHeuristicA(latencySelector, Minimizing)
    val durationA = measureLatency { placementsA(_) }

    val placementsB = placeOptimizingHeuristicB(latencySelector, Minimizing) { _ + _ }
    val durationB = measureLatency { placementsB(_) }

    (if (durationA < durationB) placementsA else placementsB) foreach { case (operator, host) =>
      system place (operator, host)
    }
  }

  def placeOptimizingBandwidth(): Unit = {
    val measureBandwidth = measure(bandwidthSelector, Maximizing, Double.MaxValue) { math.min } { avg } _

    val placementsA = placeOptimizingHeuristicA(bandwidthSelector, Maximizing)
    val bandwidthA = measureBandwidth { placementsA(_) }

    val placementsB = placeOptimizingHeuristicB(bandwidthSelector, Maximizing) { math.min }
    val bandwidthB = measureBandwidth { placementsB(_) }

    (if (bandwidthA > bandwidthB) placementsA else placementsB) foreach { case (operator, host) =>
      system place (operator, host)
    }
  }

  def placeOptimizingLatencyAndBandwidth(): Unit = {
    def average(durationNumerics: Seq[(Duration, Double)]): (Duration, Double) =
      durationNumerics.unzip match { case (latencies, bandwidths) => (avg(latencies), avg(bandwidths)) }

    def merge(durationNumeric0: (Duration, Double), durationNumeric1: (Duration, Double)): (Duration, Double) =
      (durationNumeric0, durationNumeric1) match { case ((duration0, numeric0), (duration1, numeric1)) =>
        (duration0 + duration1, math.min(numeric0, numeric1))
      }

    implicit val ordering = new Ordering[(Duration, Double)] {
      def abs(x: Duration) = if (x < Duration.Zero) -x else x
      def compare(x: (Duration, Double), y: (Duration, Double)) = ((-x._1, x._2), (-y._1, y._2)) match {
        case ((d0, n0), (d1, n1)) if d0 == d1 && n0 == n1 => 0
        case ((d0, n0), (d1, n1)) if d0 < d1 && n0 < n1 => -1
        case ((d0, n0), (d1, n1)) if d0 > d1 && n0 > n1 => 1
        case ((d0, n0), (d1, n1)) =>
          math.signum((d0 - d1) / abs(d0 + d1) + (n0 - n1) / math.abs(n0 + n1)).toInt
      }
    }

    val measureBandwidth = measure(latencyBandwidthSelector, Maximizing, (Duration.Zero, Double.MaxValue)) { merge } { average } _

    val placementsA = placeOptimizingHeuristicA(latencyBandwidthSelector, Maximizing)
    val bandwidthA = measureBandwidth { placementsA(_) }

    val placementsB = placeOptimizingHeuristicB(latencyBandwidthSelector, Maximizing) { merge }
    val bandwidthB = measureBandwidth { placementsB(_) }

    (if (bandwidthA > bandwidthB) placementsA else placementsB) foreach { case (operator, host) =>
      system place (operator, host)
    }
  }

  private def placeOptimizingHeuristicA[T: Ordering](
      selector: (HostProps, Host) => T,
      optimizing: Optimizing): collection.Map[Operator, Host] = {
    val placements = mutable.Map.empty[Operator, Host]

    def placeProducersConsumers(operator: Operator, consumer: Boolean): Unit = {
      operator.dependencies foreach { placeProducersConsumers(_, consumer = false) }
      if (consumer || operator.dependencies.isEmpty)
        placements += operator -> operator.host
    }

    def placeIntermediates(operator: Operator, consumer: Boolean): Unit = {
      operator.dependencies foreach { placeIntermediates(_, consumer = false) }

      val host =
        if (!consumer && operator.dependencies.nonEmpty) {
          val valuesForHosts =
            hostProps.toSeq collect { case (host, props) if !(placements.values exists { _== host }) =>
              val propValues =
                operator.dependencies map { dependentOperator =>
                  selector(props, placements(dependentOperator))
                }

              minmax(optimizing, propValues) -> host
            }

          if (valuesForHosts.isEmpty)
            throw new UnsupportedOperationException("not enough hosts")

          val (_, host) = minmaxBy(optimizing, valuesForHosts) { case (value, _) => value }
          host
        }
        else
          operator.host

      placements += operator -> host
    }

    system.consumers foreach { placeProducersConsumers(_, consumer = true) }
    system.consumers foreach { placeIntermediates(_, consumer = true) }

    placements
  }

  private def placeOptimizingHeuristicB[T: Ordering](
      selector: (HostProps, Host) => T,
      optimizing: Optimizing)(
      merge: (T, T) => T): collection.Map[Operator, Host] = {
    val previousPlacements = mutable.Map.empty[Operator, mutable.Set[Host]]
    val placements = mutable.Map.empty[Operator, Host]

    def allOperators(operator: Operator, parent: Option[Operator]): Seq[(Operator, Option[Operator])] =
      (operator -> parent) +: (operator.dependencies flatMap { allOperators(_, Some(operator)) })

    val operators = system.consumers flatMap { allOperators(_, None) }
    operators foreach { case (operator, _) =>
      placements += operator -> operator.host
      previousPlacements += operator -> mutable.Set(operator.host)
    }

    @tailrec def placeOperators(): Unit = {
      val changed = operators map {
        case (operator, Some(parent)) if operator.dependencies.nonEmpty =>
          val valuesForHosts =
            hostProps.toSeq collect { case (host, props) if !(placements.values exists { _ == host }) && !(previousPlacements(operator) contains host) =>
              merge(
                minmax(optimizing, operator.dependencies map { dependentOperator =>
                  selector(props, placements(dependentOperator))
                }),
                selector(hostProps(placements(parent)), host)) -> host
            }

          val currentValue =
            merge(
              minmax(optimizing, operator.dependencies map { dependency =>
                selector(hostProps(placements(operator)), placements(dependency))
              }),
              selector(hostProps(placements(parent)), placements(operator)))

          val noPotentialPlacements =
            if (valuesForHosts.isEmpty) {
              if ((hostProps.keySet -- placements.values --previousPlacements(operator)).isEmpty)
                true
              else
                throw new UnsupportedOperationException("not enough hosts")
            }
            else
              false

          if (!noPotentialPlacements) {
            val (value, host) = minmaxBy(optimizing, valuesForHosts) { case (value, _) => value }

            val changePlacement = value < currentValue
            if (changePlacement) {
              placements += operator -> host
              previousPlacements(operator) += host
            }

            changePlacement
          }
          else
            false

        case _ =>
          false
      }

      if (changed contains true)
        placeOperators()
    }

    placeOperators()

    placements
  }

  private def minmax[T: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T]) = optimizing match {
    case Maximizing => traversable.min
    case Minimizing => traversable.max
  }

  private def minmaxBy[T, U: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T])(f: T => U) = optimizing match {
    case Maximizing => traversable maxBy f
    case Minimizing => traversable minBy f
  }
}
