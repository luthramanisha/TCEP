package tcep.system

trait Host

object NoHost extends Host

trait Operator {
  val host: Host
  val dependencies: Seq[Operator]
}


//TODO: merge with the Operator Trait
trait PietzuchOperator {
  val parents: Seq[PietzuchOperator]
  val children: Seq[PietzuchOperator]
}
