package tcep.simulation.adaptive.cep

import scala.math.Ordering.Implicits.infixOrderingOps

case class ContinuousBoundedValue[T: Ordering](
    value: T, min: T, max: T,
    nextStepAmountCount: () => (T, Int),
    progress: Option[(T, Int)] = None)(
    implicit add: (T, T) => T) {
  def advance: ContinuousBoundedValue[T] = {
    val (newValue, newChange, newSteps) = progress flatMap { case (stepAmount, stepCount) =>
      val (newValue, newStepCount) = (add(value, stepAmount), stepCount - 1)
      if (newValue < min || newValue > max || newStepCount < 0)
        None
      else
        Some((newValue, stepAmount, newStepCount))
    } getOrElse {
      val (amount, count) = nextStepAmountCount()
      (value, amount, count)
    }

    copy(value = newValue, progress = Some((newChange, newSteps)))
  }
}
