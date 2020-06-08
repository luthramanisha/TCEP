package tcep.data

import java.time.Duration

import akka.actor.ActorContext
import tcep.data.Events.Event
import tcep.data.Structures.MachineLoad
import tcep.dsl.Dsl.{Frequency, FrequencyMeasurement, LatencyMeasurement, LoadMeasurement, MessageHopsMeasurement}

import scala.collection.mutable
import scala.collection.mutable.HashMap


/**
  * Helpers object for defining/representing queries in terms of case class.
  * */
object Queries {

  sealed trait NStream { val publisherName: String }
  case class NStream1[A]                (publisherName: String) extends NStream
  case class NStream2[A, B]             (publisherName: String) extends NStream
  case class NStream3[A, B, C]          (publisherName: String) extends NStream
  case class NStream4[A, B, C, D]       (publisherName: String) extends NStream
  case class NStream5[A, B, C, D, E]    (publisherName: String) extends NStream

  sealed trait Window
  case class SlidingInstances  (instances: Int) extends Window
  case class TumblingInstances (instances: Int) extends Window
  case class SlidingTime       (time: Int)   extends Window
  case class TumblingTime      (time: Int)   extends Window

  sealed trait Operator
  case object Equal        extends Operator
  case object NotEqual     extends Operator
  case object Greater      extends Operator
  case object GreaterEqual extends Operator
  case object Smaller      extends Operator
  case object SmallerEqual extends Operator

  case class NodeData(name: String, query: Query, context: ActorContext)

  sealed abstract class Requirement(val name: String)
  case class LatencyRequirement   (operator: Operator, latency: Duration, otherwise: Option[LatencyMeasurement], override val name: String = LatencyRequirement.name)      extends Requirement(name)
  object LatencyRequirement { val name = "latency" }

  case class FrequencyRequirement (operator: Operator, frequency: Frequency, otherwise: Option[FrequencyMeasurement], override val name: String = FrequencyRequirement.name) extends Requirement(name)
  object FrequencyRequirement { val name = "frequency" }

  case class LoadRequirement      (operator: Operator, machineLoad: MachineLoad, otherwise: Option[LoadMeasurement], override val name: String = LoadRequirement.name)            extends Requirement(name)
  object LoadRequirement { val name = "machineLoad" }

  case class MessageHopsRequirement(operator: Operator, requirement: Int, otherwise: Option[MessageHopsMeasurement], override val name: String = MessageHopsRequirement.name)            extends Requirement(name)
  object MessageHopsRequirement { val name = "messageHops" }

  case class ReliabilityRequirement(override val name: String = ReliabilityRequirement.name) extends Requirement(name)
  object ReliabilityRequirement { val name = "reliability" }

  def pullRequirements(q: Query, accumulatedReq: List[Requirement]): Set[Requirement] = q match {
    case query: LeafQuery => q.requirements ++ accumulatedReq
    case query: UnaryQuery => q.requirements ++ pullRequirements(query.sq, accumulatedReq)
    case query: BinaryQuery => {
      val child1 = pullRequirements(query.sq1, q.requirements.toList ++ accumulatedReq)
      val child2 = pullRequirements(query.sq2, q.requirements.toList ++ accumulatedReq)
      q.requirements ++ child1 ++ child2
    }
  }

  sealed trait Query { val requirements: Set[Requirement] }

  sealed trait LeafQuery   extends Query
  sealed trait UnaryQuery  extends Query { val sq: Query }
  sealed trait BinaryQuery extends Query { val sq1: Query; val sq2: Query }

  sealed trait StreamQuery      extends LeafQuery   { val publisherName: String }
  sealed trait SequenceQuery    extends LeafQuery   { val s1: NStream; val s2: NStream }
  sealed trait FilterQuery      extends UnaryQuery  { val cond: Event => Boolean }
  sealed trait DropElemQuery    extends UnaryQuery
  sealed trait SelfJoinQuery    extends UnaryQuery  { val w1: Window; val w2: Window }
  sealed trait AverageQuery     extends UnaryQuery  { val sectionFilter: Option[Int] = None }
  sealed trait ObserveChangeQuery extends UnaryQuery
  sealed trait SlidingWindowQuery extends UnaryQuery { val sectionFilter: Option[Int] = None; val windowSize: Int; val stepSize: Int }
  sealed trait NewAverageQuery extends UnaryQuery
  sealed trait DatabaseJoinQuery extends UnaryQuery { val db: HashMap[Int,Int]}
  sealed trait ConverterQuery extends UnaryQuery
  sealed trait WindowStatisticQuery extends UnaryQuery {val windowSize: Int}
  sealed trait ShrinkingFilterQuery extends UnaryQuery { val cond: Any => Boolean; val emitAlways: Option[Boolean] = None }
  sealed trait JoinQuery        extends BinaryQuery { val w1: Window; val w2: Window }
  sealed trait ConjunctionQuery extends BinaryQuery
  sealed trait DisjunctionQuery extends BinaryQuery

  sealed trait Query1[A]                extends Query
  sealed trait Query2[A, B]             extends Query
  sealed trait Query3[A, B, C]          extends Query
  sealed trait Query4[A, B, C, D]       extends Query
  sealed trait Query5[A, B, C, D, E]    extends Query
  sealed trait Query6[A, B, C, D, E, F] extends Query

  case class Stream1[A]                (publisherName: String, requirements: Set[Requirement]) extends Query1[A]                with StreamQuery
  case class Stream2[A, B]             (publisherName: String, requirements: Set[Requirement]) extends Query2[A, B]             with StreamQuery
  case class Stream3[A, B, C]          (publisherName: String, requirements: Set[Requirement]) extends Query3[A, B, C]          with StreamQuery
  case class Stream4[A, B, C, D]       (publisherName: String, requirements: Set[Requirement]) extends Query4[A, B, C, D]       with StreamQuery
  case class Stream5[A, B, C, D, E]    (publisherName: String, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with StreamQuery
  case class Stream6[A, B, C, D, E, F] (publisherName: String, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with StreamQuery

  case class Sequence11[A, B]             (s1: NStream1[A],             s2: NStream1[B],             requirements: Set[Requirement]) extends Query2[A, B]             with SequenceQuery
  case class Sequence12[A, B, C]          (s1: NStream1[A],             s2: NStream2[B, C],          requirements: Set[Requirement]) extends Query3[A, B, C]          with SequenceQuery
  case class Sequence21[A, B, C]          (s1: NStream2[A, B],          s2: NStream1[C],             requirements: Set[Requirement]) extends Query3[A, B, C]          with SequenceQuery
  case class Sequence13[A, B, C, D]       (s1: NStream1[A],             s2: NStream3[B, C, D],       requirements: Set[Requirement]) extends Query4[A, B, C, D]       with SequenceQuery
  case class Sequence22[A, B, C, D]       (s1: NStream2[A, B],          s2: NStream2[C, D],          requirements: Set[Requirement]) extends Query4[A, B, C, D]       with SequenceQuery
  case class Sequence31[A, B, C, D]       (s1: NStream3[A, B, C],       s2: NStream1[D],             requirements: Set[Requirement]) extends Query4[A, B, C, D]       with SequenceQuery
  case class Sequence14[A, B, C, D, E]    (s1: NStream1[A],             s2: NStream4[B, C, D, E],    requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with SequenceQuery
  case class Sequence23[A, B, C, D, E]    (s1: NStream2[A, B],          s2: NStream3[C, D, E],       requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with SequenceQuery
  case class Sequence32[A, B, C, D, E]    (s1: NStream3[A, B, C],       s2: NStream2[D, E],          requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with SequenceQuery
  case class Sequence41[A, B, C, D, E]    (s1: NStream4[A, B, C, D],    s2: NStream1[E],             requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with SequenceQuery
  case class Sequence15[A, B, C, D, E, F] (s1: NStream1[A],             s2: NStream5[B, C, D, E, F], requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with SequenceQuery
  case class Sequence24[A, B, C, D, E, F] (s1: NStream2[A, B],          s2: NStream4[C, D, E, F],    requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with SequenceQuery
  case class Sequence33[A, B, C, D, E, F] (s1: NStream3[A, B, C],       s2: NStream3[D, E, F],       requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with SequenceQuery
  case class Sequence42[A, B, C, D, E, F] (s1: NStream4[A, B, C, D],    s2: NStream2[E, F],          requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with SequenceQuery
  case class Sequence51[A, B, C, D, E, F] (s1: NStream5[A, B, C, D, E], s2: NStream1[F],             requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with SequenceQuery

  case class Filter1[A]                (sq: Query1[A],                cond: Event => Boolean, requirements: Set[Requirement]) extends Query1[A]                with FilterQuery
  case class Filter2[A, B]             (sq: Query2[A, B],             cond: Event => Boolean, requirements: Set[Requirement]) extends Query2[A, B]             with FilterQuery
  case class Filter3[A, B, C]          (sq: Query3[A, B, C],          cond: Event => Boolean, requirements: Set[Requirement]) extends Query3[A, B, C]          with FilterQuery
  case class Filter4[A, B, C, D]       (sq: Query4[A, B, C, D],       cond: Event => Boolean, requirements: Set[Requirement]) extends Query4[A, B, C, D]       with FilterQuery
  case class Filter5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    cond: Event => Boolean, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with FilterQuery
  case class Filter6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], cond: Event => Boolean, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with FilterQuery

  case class DropElem1Of2[A, B]             (sq: Query2[A, B],             requirements: Set[Requirement]) extends Query1[B]             with DropElemQuery
  case class DropElem2Of2[A, B]             (sq: Query2[A, B],             requirements: Set[Requirement]) extends Query1[A]             with DropElemQuery
  case class DropElem1Of3[A, B, C]          (sq: Query3[A, B, C],          requirements: Set[Requirement]) extends Query2[B, C]          with DropElemQuery
  case class DropElem2Of3[A, B, C]          (sq: Query3[A, B, C],          requirements: Set[Requirement]) extends Query2[A, C]          with DropElemQuery
  case class DropElem3Of3[A, B, C]          (sq: Query3[A, B, C],          requirements: Set[Requirement]) extends Query2[A, B]          with DropElemQuery
  case class DropElem1Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement]) extends Query3[B, C, D]       with DropElemQuery
  case class DropElem2Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement]) extends Query3[A, C, D]       with DropElemQuery
  case class DropElem3Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement]) extends Query3[A, B, D]       with DropElemQuery
  case class DropElem4Of4[A, B, C, D]       (sq: Query4[A, B, C, D],       requirements: Set[Requirement]) extends Query3[A, B, C]       with DropElemQuery
  case class DropElem1Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement]) extends Query4[B, C, D, E]    with DropElemQuery
  case class DropElem2Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement]) extends Query4[A, C, D, E]    with DropElemQuery
  case class DropElem3Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement]) extends Query4[A, B, D, E]    with DropElemQuery
  case class DropElem4Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement]) extends Query4[A, B, C, E]    with DropElemQuery
  case class DropElem5Of5[A, B, C, D, E]    (sq: Query5[A, B, C, D, E],    requirements: Set[Requirement]) extends Query4[A, B, C, D]    with DropElemQuery
  case class DropElem1Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[B, C, D, E, F] with DropElemQuery
  case class DropElem2Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[A, C, D, E, F] with DropElemQuery
  case class DropElem3Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[A, B, D, E, F] with DropElemQuery
  case class DropElem4Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[A, B, C, E, F] with DropElemQuery
  case class DropElem5Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[A, B, C, D, F] with DropElemQuery
  case class DropElem6Of6[A, B, C, D, E, F] (sq: Query6[A, B, C, D, E, F], requirements: Set[Requirement]) extends Query5[A, B, C, D, E] with DropElemQuery

  case class SelfJoin11[A]       (sq: Query1[A],       w1: Window, w2: Window, requirements: Set[Requirement]) extends Query2[A, A]             with SelfJoinQuery
  case class SelfJoin22[A, B]    (sq: Query2[A, B],    w1: Window, w2: Window, requirements: Set[Requirement]) extends Query4[A, B, A, B]       with SelfJoinQuery
  case class SelfJoin33[A, B, C] (sq: Query3[A, B, C], w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, A, B, C] with SelfJoinQuery

  case class Join11[A, B]             (sq1: Query1[A],             sq2: Query1[B],             w1: Window, w2: Window, requirements: Set[Requirement]) extends Query2[A, B]             with JoinQuery
  case class Join12[A, B, C]          (sq1: Query1[A],             sq2: Query2[B, C],          w1: Window, w2: Window, requirements: Set[Requirement]) extends Query3[A, B, C]          with JoinQuery
  case class Join21[A, B, C]          (sq1: Query2[A, B],          sq2: Query1[C],             w1: Window, w2: Window, requirements: Set[Requirement]) extends Query3[A, B, C]          with JoinQuery
  case class Join13[A, B, C, D]       (sq1: Query1[A],             sq2: Query3[B, C, D],       w1: Window, w2: Window, requirements: Set[Requirement]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join22[A, B, C, D]       (sq1: Query2[A, B],          sq2: Query2[C, D],          w1: Window, w2: Window, requirements: Set[Requirement]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join31[A, B, C, D]       (sq1: Query3[A, B, C],       sq2: Query1[D],             w1: Window, w2: Window, requirements: Set[Requirement]) extends Query4[A, B, C, D]       with JoinQuery
  case class Join14[A, B, C, D, E]    (sq1: Query1[A],             sq2: Query4[B, C, D, E],    w1: Window, w2: Window, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join23[A, B, C, D, E]    (sq1: Query2[A, B],          sq2: Query3[C, D, E],       w1: Window, w2: Window, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join32[A, B, C, D, E]    (sq1: Query3[A, B, C],       sq2: Query2[D, E],          w1: Window, w2: Window, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join41[A, B, C, D, E]    (sq1: Query4[A, B, C, D],    sq2: Query1[E],             w1: Window, w2: Window, requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with JoinQuery
  case class Join15[A, B, C, D, E, F] (sq1: Query1[A],             sq2: Query5[B, C, D, E, F], w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join24[A, B, C, D, E, F] (sq1: Query2[A, B],          sq2: Query4[C, D, E, F],    w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join33[A, B, C, D, E, F] (sq1: Query3[A, B, C],       sq2: Query3[D, E, F],       w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join42[A, B, C, D, E, F] (sq1: Query4[A, B, C, D],    sq2: Query2[E, F],          w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with JoinQuery
  case class Join51[A, B, C, D, E, F] (sq1: Query5[A, B, C, D, E], sq2: Query1[F],             w1: Window, w2: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with JoinQuery

  case class Conjunction11[A, B]             (sq1: Query1[A],             sq2: Query1[B],             requirements: Set[Requirement]) extends Query2[A, B]             with ConjunctionQuery
  case class Conjunction12[A, B, C]          (sq1: Query1[A],             sq2: Query2[B, C],          requirements: Set[Requirement]) extends Query3[A, B, C]          with ConjunctionQuery
  case class Conjunction21[A, B, C]          (sq1: Query2[A, B],          sq2: Query1[C],             requirements: Set[Requirement]) extends Query3[A, B, C]          with ConjunctionQuery
  case class Conjunction13[A, B, C, D]       (sq1: Query1[A],             sq2: Query3[B, C, D],       requirements: Set[Requirement]) extends Query4[A, B, C, D]       with ConjunctionQuery
  case class Conjunction22[A, B, C, D]       (sq1: Query2[A, B],          sq2: Query2[C, D],          requirements: Set[Requirement]) extends Query4[A, B, C, D]       with ConjunctionQuery
  case class Conjunction31[A, B, C, D]       (sq1: Query3[A, B, C],       sq2: Query1[D],             requirements: Set[Requirement]) extends Query4[A, B, C, D]       with ConjunctionQuery
  case class Conjunction14[A, B, C, D, E]    (sq1: Query1[A],             sq2: Query4[B, C, D, E],    requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with ConjunctionQuery
  case class Conjunction23[A, B, C, D, E]    (sq1: Query2[A, B],          sq2: Query3[C, D, E],       requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with ConjunctionQuery
  case class Conjunction32[A, B, C, D, E]    (sq1: Query3[A, B, C],       sq2: Query2[D, E],          requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with ConjunctionQuery
  case class Conjunction41[A, B, C, D, E]    (sq1: Query4[A, B, C, D],    sq2: Query1[E],             requirements: Set[Requirement]) extends Query5[A, B, C, D, E]    with ConjunctionQuery
  case class Conjunction15[A, B, C, D, E, F] (sq1: Query1[A],             sq2: Query5[B, C, D, E, F], requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with ConjunctionQuery
  case class Conjunction24[A, B, C, D, E, F] (sq1: Query2[A, B],          sq2: Query4[C, D, E, F],    requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with ConjunctionQuery
  case class Conjunction33[A, B, C, D, E, F] (sq1: Query3[A, B, C],       sq2: Query3[D, E, F],       requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with ConjunctionQuery
  case class Conjunction42[A, B, C, D, E, F] (sq1: Query4[A, B, C, D],    sq2: Query2[E, F],          requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with ConjunctionQuery
  case class Conjunction51[A, B, C, D, E, F] (sq1: Query5[A, B, C, D, E], sq2: Query1[F],             requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with ConjunctionQuery

  type X = Unit
  case class Disjunction11[A, B]                               (sq1: Query1[A],                sq2: Query1[B],                requirements: Set[Requirement]) extends Query1[Either[A, B]]                                                                       with DisjunctionQuery
  case class Disjunction12[A, B, C]                            (sq1: Query1[A],                sq2: Query2[B, C],             requirements: Set[Requirement]) extends Query2[Either[A, B], Either[X, C]]                                                         with DisjunctionQuery
  case class Disjunction13[A, B, C, D]                         (sq1: Query1[A],                sq2: Query3[B, C, D],          requirements: Set[Requirement]) extends Query3[Either[A, B], Either[X, C], Either[X, D]]                                           with DisjunctionQuery
  case class Disjunction14[A, B, C, D, E]                      (sq1: Query1[A],                sq2: Query4[B, C, D, E],       requirements: Set[Requirement]) extends Query4[Either[A, B], Either[X, C], Either[X, D], Either[X, E]]                             with DisjunctionQuery
  case class Disjunction15[A, B, C, D, E, F]                   (sq1: Query1[A],                sq2: Query5[B, C, D, E, F],    requirements: Set[Requirement]) extends Query5[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F]]               with DisjunctionQuery
  case class Disjunction16[A, B, C, D, E, F, G]                (sq1: Query1[A],                sq2: Query6[B, C, D, E, F, G], requirements: Set[Requirement]) extends Query6[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F], Either[X, G]] with DisjunctionQuery
  case class Disjunction21[A, B, C]                            (sq1: Query2[A, B],             sq2: Query1[C],                requirements: Set[Requirement]) extends Query2[Either[A, C], Either[B, X]]                                                         with DisjunctionQuery
  case class Disjunction22[A, B, C, D]                         (sq1: Query2[A, B],             sq2: Query2[C, D],             requirements: Set[Requirement]) extends Query2[Either[A, C], Either[B, D]]                                                         with DisjunctionQuery
  case class Disjunction23[A, B, C, D, E]                      (sq1: Query2[A, B],             sq2: Query3[C, D, E],          requirements: Set[Requirement]) extends Query3[Either[A, C], Either[B, D], Either[X, E]]                                           with DisjunctionQuery
  case class Disjunction24[A, B, C, D, E, F]                   (sq1: Query2[A, B],             sq2: Query4[C, D, E, F],       requirements: Set[Requirement]) extends Query4[Either[A, C], Either[B, D], Either[X, E], Either[X, F]]                             with DisjunctionQuery
  case class Disjunction25[A, B, C, D, E, F, G]                (sq1: Query2[A, B],             sq2: Query5[C, D, E, F, G],    requirements: Set[Requirement]) extends Query5[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G]]               with DisjunctionQuery
  case class Disjunction26[A, B, C, D, E, F, G, H]             (sq1: Query2[A, B],             sq2: Query6[C, D, E, F, G, H], requirements: Set[Requirement]) extends Query6[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G], Either[X, H]] with DisjunctionQuery
  case class Disjunction31[A, B, C, D]                         (sq1: Query3[A, B, C],          sq2: Query1[D],                requirements: Set[Requirement]) extends Query3[Either[A, D], Either[B, X], Either[C, X]]                                           with DisjunctionQuery
  case class Disjunction32[A, B, C, D, E]                      (sq1: Query3[A, B, C],          sq2: Query2[D, E],             requirements: Set[Requirement]) extends Query3[Either[A, D], Either[B, E], Either[C, X]]                                           with DisjunctionQuery
  case class Disjunction33[A, B, C, D, E, F]                   (sq1: Query3[A, B, C],          sq2: Query3[D, E, F],          requirements: Set[Requirement]) extends Query3[Either[A, D], Either[B, E], Either[C, F]]                                           with DisjunctionQuery
  case class Disjunction34[A, B, C, D, E, F, G]                (sq1: Query3[A, B, C],          sq2: Query4[D, E, F, G],       requirements: Set[Requirement]) extends Query4[Either[A, D], Either[B, E], Either[C, F], Either[X, G]]                             with DisjunctionQuery
  case class Disjunction35[A, B, C, D, E, F, G, H]             (sq1: Query3[A, B, C],          sq2: Query5[D, E, F, G, H],    requirements: Set[Requirement]) extends Query5[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H]]               with DisjunctionQuery
  case class Disjunction36[A, B, C, D, E, F, G, H, I]          (sq1: Query3[A, B, C],          sq2: Query6[D, E, F, G, H, I], requirements: Set[Requirement]) extends Query6[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H], Either[X, I]] with DisjunctionQuery
  case class Disjunction41[A, B, C, D, E]                      (sq1: Query4[A, B, C, D],       sq2: Query1[E],                requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, X], Either[C, X], Either[D, X]]                             with DisjunctionQuery
  case class Disjunction42[A, B, C, D, E, F]                   (sq1: Query4[A, B, C, D],       sq2: Query2[E, F],             requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, F], Either[C, X], Either[D, X]]                             with DisjunctionQuery
  case class Disjunction43[A, B, C, D, E, F, G]                (sq1: Query4[A, B, C, D],       sq2: Query3[E, F, G],          requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, X]]                             with DisjunctionQuery
  case class Disjunction44[A, B, C, D, E, F, G, H]             (sq1: Query4[A, B, C, D],       sq2: Query4[E, F, G, H],       requirements: Set[Requirement]) extends Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, H]]                             with DisjunctionQuery
  case class Disjunction45[A, B, C, D, E, F, G, H, I]          (sq1: Query4[A, B, C, D],       sq2: Query5[E, F, G, H, I],    requirements: Set[Requirement]) extends Query5[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I]]               with DisjunctionQuery
  case class Disjunction46[A, B, C, D, E, F, G, H, I, J]       (sq1: Query4[A, B, C, D],       sq2: Query6[E, F, G, H, I, J], requirements: Set[Requirement]) extends Query6[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I], Either[X, J]] with DisjunctionQuery
  case class Disjunction51[A, B, C, D, E, F]                   (sq1: Query5[A, B, C, D, E],    sq2: Query1[F],                requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X]]               with DisjunctionQuery
  case class Disjunction52[A, B, C, D, E, F, G]                (sq1: Query5[A, B, C, D, E],    sq2: Query2[F, G],             requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X]]               with DisjunctionQuery
  case class Disjunction53[A, B, C, D, E, F, G, H]             (sq1: Query5[A, B, C, D, E],    sq2: Query3[F, G, H],          requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X]]               with DisjunctionQuery
  case class Disjunction54[A, B, C, D, E, F, G, H, I]          (sq1: Query5[A, B, C, D, E],    sq2: Query4[F, G, H, I],       requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X]]               with DisjunctionQuery
  case class Disjunction55[A, B, C, D, E, F, G, H, I, J]       (sq1: Query5[A, B, C, D, E],    sq2: Query5[F, G, H, I, J],    requirements: Set[Requirement]) extends Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J]]               with DisjunctionQuery
  case class Disjunction56[A, B, C, D, E, F, G, H, I, J, K]    (sq1: Query5[A, B, C, D, E],    sq2: Query6[F, G, H, I, J, K], requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[X, K]] with DisjunctionQuery
  case class Disjunction61[A, B, C, D, E, F, G]                (sq1: Query6[A, B, C, D, E, F], sq2: Query1[G],                requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction62[A, B, C, D, E, F, G, H]             (sq1: Query6[A, B, C, D, E, F], sq2: Query2[G, H],             requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction63[A, B, C, D, E, F, G, H, I]          (sq1: Query6[A, B, C, D, E, F], sq2: Query3[G, H, I],          requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction64[A, B, C, D, E, F, G, H, I, J]       (sq1: Query6[A, B, C, D, E, F], sq2: Query4[G, H, I, J],       requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X], Either[F, X]] with DisjunctionQuery
  case class Disjunction65[A, B, C, D, E, F, G, H, I, J, K]    (sq1: Query6[A, B, C, D, E, F], sq2: Query5[G, H, I, J, K],    requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, X]] with DisjunctionQuery
  case class Disjunction66[A, B, C, D, E, F, G, H, I, J, K, L] (sq1: Query6[A, B, C, D, E, F], sq2: Query6[G, H, I, J, K, L], requirements: Set[Requirement]) extends Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, K]] with DisjunctionQuery

  case class ObserveChange1[A] (sq: Query1[A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ObserveChangeQuery
  case class ObserveChange2[A] (sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ObserveChangeQuery
  case class ObserveChange3[A] (sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ObserveChangeQuery
  case class ObserveChange4[A] (sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ObserveChangeQuery
  case class ObserveChange5[A] (sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ObserveChangeQuery
  case class ObserveChange6[A] (sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ObserveChangeQuery

  case class SlidingWindow1[A] (sq: Query1[A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1) extends Query1[A] with SlidingWindowQuery
  case class SlidingWindow2[A] (sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1) extends Query1[A] with SlidingWindowQuery
  case class SlidingWindow3[A] (sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1) extends Query1[A] with SlidingWindowQuery
  case class SlidingWindow4[A] (sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1) extends Query1[A] with SlidingWindowQuery
  case class SlidingWindow5[A] (sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1) extends Query1[A] with SlidingWindowQuery
  case class SlidingWindow6[A] (sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None, override val windowSize: Int, override val stepSize: Int = 1) extends Query1[A] with SlidingWindowQuery

  case class NewAverage1[A] (sq: Query1[A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with NewAverageQuery
  case class NewAverage2[A] (sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with NewAverageQuery
  case class NewAverage3[A] (sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with NewAverageQuery
  case class NewAverage4[A] (sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with NewAverageQuery
  case class NewAverage5[A] (sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with NewAverageQuery
  case class NewAverage6[A] (sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with NewAverageQuery

  case class ShrinkFilter2[A] (sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement](), cond: Any => Boolean, override val emitAlways: Option[Boolean] = None) extends Query1[A] with ShrinkingFilterQuery
  case class ShrinkFilter3[A] (sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement](), cond: Any => Boolean, override val emitAlways: Option[Boolean] = None) extends Query1[A] with ShrinkingFilterQuery
  case class ShrinkFilter4[A] (sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), cond: Any => Boolean, override val emitAlways: Option[Boolean] = None) extends Query1[A] with ShrinkingFilterQuery
  case class ShrinkFilter5[A] (sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), cond: Any => Boolean, override val emitAlways: Option[Boolean] = None) extends Query1[A] with ShrinkingFilterQuery
  case class ShrinkFilter6[A] (sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), cond: Any => Boolean, override val emitAlways: Option[Boolean] = None) extends Query1[A] with ShrinkingFilterQuery

  case class DatabaseJoin1[A] ( sq: Query1[A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int]) extends Query1[A] with DatabaseJoinQuery
  case class DatabaseJoin2[A] ( sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int]) extends Query1[A] with DatabaseJoinQuery
  case class DatabaseJoin3[A] ( sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int]) extends Query1[A] with DatabaseJoinQuery
  case class DatabaseJoin4[A] ( sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int]) extends Query1[A] with DatabaseJoinQuery
  case class DatabaseJoin5[A] ( sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int]) extends Query1[A] with DatabaseJoinQuery
  case class DatabaseJoin6[A] ( sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val db: mutable.HashMap[Int, Int]) extends Query1[A] with DatabaseJoinQuery

  case class Convert1[A] ( sq: Query1[A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ConverterQuery
  case class Convert2[A] ( sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ConverterQuery
  case class Convert3[A] ( sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ConverterQuery
  case class Convert4[A] ( sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ConverterQuery
  case class Convert5[A] ( sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ConverterQuery
  case class Convert6[A] ( sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement]()) extends Query1[A] with ConverterQuery

  case class WindowStatistic1[A] ( sq: Query1[A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int) extends Query1[A] with WindowStatisticQuery
  case class WindowStatistic2[A] ( sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int) extends Query1[A] with WindowStatisticQuery
  case class WindowStatistic3[A] ( sq: Query3[A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int) extends Query1[A] with WindowStatisticQuery
  case class WindowStatistic4[A] ( sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int) extends Query1[A] with WindowStatisticQuery
  case class WindowStatistic5[A] ( sq: Query5[A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int) extends Query1[A] with WindowStatisticQuery
  case class WindowStatistic6[A] ( sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val windowSize: Int) extends Query1[A] with WindowStatisticQuery

  case class Average2[A] (sq: Query2[A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None) extends Query1[A] with AverageQuery
  case class Average4[A] (sq: Query4[A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None) extends Query1[A] with AverageQuery
  case class Average6[A] (sq: Query6[A, A, A, A, A, A], requirements: Set[Requirement] = Set[Requirement](), override val sectionFilter: Option[Int] = None) extends Query1[A] with AverageQuery
  // a join of six unary queries
  //case class SenaryJoin[A, B, C, D, E, F] (sq1: Query1[A], sq2: Query1[B], sq3: Query1[C], sq4: Query1[D], sq5: Query1[E], sq6: Query1[F], w1: Window, w2: Window, w3: Window, w4: Window, w5: Window, w6: Window, requirements: Set[Requirement]) extends Query6[A, B, C, D, E, F] with SenaryJoinQuery

  def queryToString(q: Query): String = {
    q match {
      case bq: BinaryQuery => s"${bq.getClass.getSimpleName}[${queryToString(bq.sq1)}, ${queryToString(bq.sq2)}]"
      case uq: UnaryQuery => s"${uq.getClass.getSimpleName}[${queryToString(uq.sq)}]"
      case sq: StreamQuery => s"${sq.getClass.getSimpleName}[${sq.publisherName}]"
      case seq: SequenceQuery => s"${seq.getClass.getSimpleName}[${seq.s1.publisherName}, ${seq.s2.publisherName}]"
    }
  }
}
