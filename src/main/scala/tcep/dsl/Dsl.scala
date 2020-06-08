package tcep.dsl

import java.time.Duration

import tcep.data.Events._
import tcep.data.Queries._
import tcep.data.Structures.MachineLoad

/**
  * Domain specific language - Helpers class to express query results.
  *
  * The query under the hood uses case class representation from the package
  * [[tcep.data.Events]] & [[tcep.data.Queries]]
  * */

object Dsl {

  trait Timespan
  case class Nanoseconds(i: Int) extends Timespan
  case class Milliseconds(i: Int) extends Timespan
  case class Seconds(i: Int) extends Timespan
  case class Minutes(i: Int) extends Timespan

  case class TimespanHelper(i: Int) {
    def nanoseconds: Nanoseconds = Nanoseconds(i)
    def milliseconds: Milliseconds = Milliseconds(i)
    def seconds: Seconds = Seconds(i)
    def minutes: Minutes = Minutes(i)
  }

  implicit def intToTimespanHelper(i: Int): TimespanHelper = TimespanHelper(i)

  case class Instances(i: Int)

  case class InstancesHelper(i: Int) {
    def instances: Instances = Instances(i)
  }

  implicit def intToInstancesHelper(i: Int): InstancesHelper = InstancesHelper(i)


  def slidingWindow  (instances: Instances):            Window = SlidingInstances  (instances.i)
  def slidingWindow  (milliseconds: Milliseconds):      Window = SlidingTime       (milliseconds.i)
  def slidingWindow  (seconds: Seconds):                Window = SlidingTime       (seconds.i)
  def slidingWindow  (minutes: Minutes):                Window = SlidingTime       (minutes.i)

  def tumblingWindow (instances: Instances): Window = TumblingInstances (instances.i)
  def tumblingWindow (seconds: Seconds):     Window = TumblingTime      (seconds.i)
  def tumblingWindow (milliseconds: Milliseconds):     Window = TumblingTime      (milliseconds.i)

  def nStream[A]                (publisherName: String): NStream1[A] =                NStream1 (publisherName)
  def nStream[A, B]             (publisherName: String): NStream2[A, B] =             NStream2 (publisherName)
  def nStream[A, B, C]          (publisherName: String): NStream3[A, B, C] =          NStream3 (publisherName)
  def nStream[A, B, C, D]       (publisherName: String): NStream4[A, B, C, D] =       NStream4 (publisherName)
  def nStream[A, B, C, D, E]    (publisherName: String): NStream5[A, B, C, D, E] =    NStream5 (publisherName)

  case class Ratio(instances: Instances, seconds: Seconds)
  case class Frequency(frequency: Int, interval: Int)
  def ratio(instances: Instances, seconds: Seconds): Ratio = Ratio(instances, seconds)

  def frequency: FrequencyHelper.type = FrequencyHelper

  case object FrequencyHelper {
    def === (frequency: Frequency): FrequencyHelper2 = FrequencyHelper2(Equal, frequency)
    def =!= (frequency: Frequency): FrequencyHelper2 = FrequencyHelper2(NotEqual, frequency)
    def >   (frequency: Frequency): FrequencyHelper2 = FrequencyHelper2(Greater, frequency)
    def >=  (frequency: Frequency): FrequencyHelper2 = FrequencyHelper2(GreaterEqual, frequency)
    def <   (frequency: Frequency): FrequencyHelper2 = FrequencyHelper2(Smaller, frequency)
    def <=  (frequency: Frequency): FrequencyHelper2 = FrequencyHelper2(SmallerEqual, frequency)
  }

  case class FrequencyHelper2(operator: Operator, frequency: Frequency) {
    def otherwise[A <: FrequencyMeasurement](otherwise: Option[A]): FrequencyRequirement =
      FrequencyRequirement(operator, frequency, otherwise)
  }


  def timespan(timespan: Timespan): Duration = timespan match {
    case Nanoseconds(i) => Duration.ofNanos(i)
    case Milliseconds(i) => Duration.ofMillis(i)
    case Seconds(i) => Duration.ofSeconds(i)
  }

  def latency: LatencyHelper.type = LatencyHelper
  def load: LoadHelper.type = LoadHelper
  def hops: MessageHopsHelper.type = MessageHopsHelper

  case object MessageHopsHelper {
    def === (hops: Int): MessageHopsHelper2 = MessageHopsHelper2 (Equal, hops)
    def =!= (hops: Int): MessageHopsHelper2 = MessageHopsHelper2 (NotEqual, hops)
    def >   (hops: Int): MessageHopsHelper2 = MessageHopsHelper2 (Greater, hops)
    def >=  (hops: Int): MessageHopsHelper2 = MessageHopsHelper2 (GreaterEqual, hops)
    def <   (hops: Int): MessageHopsHelper2 = MessageHopsHelper2 (Smaller, hops)
    def <=  (hops: Int): MessageHopsHelper2 = MessageHopsHelper2 (SmallerEqual, hops)
  }

  case object LatencyHelper {
    def === (duration: Duration): LatencyHelper2 = LatencyHelper2 (Equal, duration)
    def =!= (duration: Duration): LatencyHelper2 = LatencyHelper2 (NotEqual, duration)
    def >   (duration: Duration): LatencyHelper2 = LatencyHelper2 (Greater, duration)
    def >=  (duration: Duration): LatencyHelper2 = LatencyHelper2 (GreaterEqual, duration)
    def <   (duration: Duration): LatencyHelper2 = LatencyHelper2 (Smaller, duration)
    def <=  (duration: Duration): LatencyHelper2 = LatencyHelper2 (SmallerEqual, duration)
  }

  case object LoadHelper {
    def === (l: MachineLoad): LoadHelper2 = LoadHelper2 (Equal, l)
    def =!= (l: MachineLoad): LoadHelper2 = LoadHelper2 (NotEqual, l)
    def >   (l: MachineLoad): LoadHelper2 = LoadHelper2 (Greater, l)
    def >=  (l: MachineLoad): LoadHelper2 = LoadHelper2 (GreaterEqual, l)
    def <   (l: MachineLoad): LoadHelper2 = LoadHelper2 (Smaller, l)
    def <=  (l: MachineLoad): LoadHelper2 = LoadHelper2 (SmallerEqual, l)
  }

  sealed trait Measurement
  //Wrapping lambdas by classes as AKKA can't serialize lambda :-/
  abstract class MessageHopsMeasurement() extends Measurement {
    def apply(hops: Int): Any
  }

  case class MessageHopsHelper2(operator: Operator, hops: Int) {
    def otherwise(otherwise: Option[MessageHopsMeasurement]): MessageHopsRequirement =
      MessageHopsRequirement(operator, hops, otherwise)
  }

  abstract class LatencyMeasurement() extends Measurement {
    def apply(latency: Duration): Any
  }

  abstract class TransitionMeasurement() extends Measurement {
    // status 0 => Not in transition
    // status 1 => In transition
    def apply(status: Int): Any
  }

  abstract class NetworkUsageMeasurement() extends Measurement {
    def apply(usage: Double): Any
  }

  abstract class PublishingRateMeasurement() extends Measurement {
    def apply(rate: Double): Any
  }

  abstract class MessageOverheadMeasurement() extends Measurement {
    def apply(eventKBytes: Long, placementKBytes: Long): Any
  }

  case class LatencyHelper2(operator: Operator, latency: Duration) {
    def otherwise(otherwise: Option[LatencyMeasurement]): LatencyRequirement =
      LatencyRequirement(operator, latency, otherwise)
  }
  abstract class FrequencyMeasurement() extends Measurement {
    def apply(frequency: Int):Any
  }

  abstract class LoadMeasurement() extends Measurement {
    def apply(load: Double):Any
  }

  abstract class BandwidthMeasurement() extends Measurement {
    def apply(bandwidth: Double):Any
  }



  case class LoadHelper2(operator: Operator, load: MachineLoad) {
    def otherwise(otherwise: Option[LoadMeasurement]): LoadRequirement =
      LoadRequirement(operator, load, otherwise)
  }
  def stream[A]                (publisherName: String, requirements: Requirement*): Query1[A] =                Stream1 (publisherName, requirements.toSet)
  def stream[A, B]             (publisherName: String, requirements: Requirement*): Query2[A, B] =             Stream2 (publisherName, requirements.toSet)
  def stream[A, B, C]          (publisherName: String, requirements: Requirement*): Query3[A, B, C] =          Stream3 (publisherName, requirements.toSet)
  def stream[A, B, C, D]       (publisherName: String, requirements: Requirement*): Query4[A, B, C, D] =       Stream4 (publisherName, requirements.toSet)
  def stream[A, B, C, D, E]    (publisherName: String, requirements: Requirement*): Query5[A, B, C, D, E] =    Stream5 (publisherName, requirements.toSet)
  def stream[A, B, C, D, E, F] (publisherName: String, requirements: Requirement*): Query6[A, B, C, D, E, F] = Stream6 (publisherName, requirements.toSet)

  case class Sequence1Helper[A](s: NStream1[A]) {
    def ->[B]             (s2: NStream1[B]            ): (NStream1[A], NStream1[B]) =             (s, s2)
    def ->[B, C]          (s2: NStream2[B, C]         ): (NStream1[A], NStream2[B, C]) =          (s, s2)
    def ->[B, C, D]       (s2: NStream3[B, C, D]      ): (NStream1[A], NStream3[B, C, D]) =       (s, s2)
    def ->[B, C, D, E]    (s2: NStream4[B, C, D, E]   ): (NStream1[A], NStream4[B, C, D, E]) =    (s, s2)
    def ->[B, C, D, E, F] (s2: NStream5[B, C, D, E, F]): (NStream1[A], NStream5[B, C, D, E, F]) = (s, s2)
  }

  case class Sequence2Helper[A, B](s: NStream2[A, B]) {
    def ->[C]          (s2: NStream1[C]         ): (NStream2[A, B], NStream1[C]) =          (s, s2)
    def ->[C, D]       (s2: NStream2[C, D]      ): (NStream2[A, B], NStream2[C, D]) =       (s, s2)
    def ->[C, D, E]    (s2: NStream3[C, D, E]   ): (NStream2[A, B], NStream3[C, D, E]) =    (s, s2)
    def ->[C, D, E, F] (s2: NStream4[C, D, E, F]): (NStream2[A, B], NStream4[C, D, E, F]) = (s, s2)
  }

  case class Sequence3Helper[A, B, C](s: NStream3[A, B, C]) {
    def ->[D]       (s2: NStream1[D]      ): (NStream3[A, B, C], NStream1[D]) =       (s, s2)
    def ->[D, E]    (s2: NStream2[D, E]   ): (NStream3[A, B, C], NStream2[D, E]) =    (s, s2)
    def ->[D, E, F] (s2: NStream3[D, E, F]): (NStream3[A, B, C], NStream3[D, E, F]) = (s, s2)
  }

  case class Sequence4Helper[A, B, C, D](s: NStream4[A, B, C, D]) {
    def ->[E]    (s2: NStream1[E]   ): (NStream4[A, B, C, D], NStream1[E]) =    (s, s2)
    def ->[E, F] (s2: NStream2[E, F]): (NStream4[A, B, C, D], NStream2[E, F]) = (s, s2)
  }

  case class Sequence5Helper[A, B, C, D, E](s: NStream5[A, B, C, D, E]) {
    def ->[F] (s2: NStream1[F]): (NStream5[A, B, C, D, E], NStream1[F]) = (s, s2)
  }

  implicit def nStream1ToSequence1Helper[A]             (s: NStream1[A]):             Sequence1Helper[A] =             Sequence1Helper(s)
  implicit def nStream2ToSequence2Helper[A, B]          (s: NStream2[A, B]):          Sequence2Helper[A, B] =          Sequence2Helper(s)
  implicit def nStream3ToSequence3Helper[A, B, C]       (s: NStream3[A, B, C]):       Sequence3Helper[A, B, C] =       Sequence3Helper(s)
  implicit def nStream4ToSequence4Helper[A, B, C, D]    (s: NStream4[A, B, C, D]):    Sequence4Helper[A, B, C, D] =    Sequence4Helper(s)
  implicit def nStream5ToSequence5Helper[A, B, C, D, E] (s: NStream5[A, B, C, D, E]): Sequence5Helper[A, B, C, D, E] = Sequence5Helper(s)

  def sequence[A, B]             (tuple: (NStream1[A],             NStream1[B]),             requirements: Requirement*): Sequence11[A, B] =             Sequence11 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C]          (tuple: (NStream1[A],             NStream2[B, C]),          requirements: Requirement*): Sequence12[A, B, C] =          Sequence12 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C]          (tuple: (NStream2[A, B],          NStream1[C]),             requirements: Requirement*): Sequence21[A, B, C] =          Sequence21 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D]       (tuple: (NStream1[A],             NStream3[B, C, D]),       requirements: Requirement*): Sequence13[A, B, C, D] =       Sequence13 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D]       (tuple: (NStream2[A, B],          NStream2[C, D]),          requirements: Requirement*): Sequence22[A, B, C, D] =       Sequence22 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D]       (tuple: (NStream3[A, B, C],       NStream1[D]),             requirements: Requirement*): Sequence31[A, B, C, D] =       Sequence31 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D, E]    (tuple: (NStream1[A],             NStream4[B, C, D, E]),    requirements: Requirement*): Sequence14[A, B, C, D, E] =    Sequence14 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D, E]    (tuple: (NStream2[A, B],          NStream3[C, D, E]),       requirements: Requirement*): Sequence23[A, B, C, D, E] =    Sequence23 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D, E]    (tuple: (NStream3[A, B, C],       NStream2[D, E]),          requirements: Requirement*): Sequence32[A, B, C, D, E] =    Sequence32 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D, E]    (tuple: (NStream4[A, B, C, D],    NStream1[E]),             requirements: Requirement*): Sequence41[A, B, C, D, E] =    Sequence41 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D, E, F] (tuple: (NStream1[A],             NStream5[B, C, D, E, F]), requirements: Requirement*): Sequence15[A, B, C, D, E, F] = Sequence15 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D, E, F] (tuple: (NStream2[A, B],          NStream4[C, D, E, F]),    requirements: Requirement*): Sequence24[A, B, C, D, E, F] = Sequence24 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D, E, F] (tuple: (NStream3[A, B, C],       NStream3[D, E, F]),       requirements: Requirement*): Sequence33[A, B, C, D, E, F] = Sequence33 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D, E, F] (tuple: (NStream4[A, B, C, D],    NStream2[E, F]),          requirements: Requirement*): Sequence42[A, B, C, D, E, F] = Sequence42 (tuple._1, tuple._2, requirements.toSet)
  def sequence[A, B, C, D, E, F] (tuple: (NStream5[A, B, C, D, E], NStream1[F]),             requirements: Requirement*): Sequence51[A, B, C, D, E, F] = Sequence51 (tuple._1, tuple._2, requirements.toSet)

  case class Query1Helper[A](q: Query1[A]) {
    def where                (                              cond: (A) => Boolean,   requirements: Requirement*): Query1[A] =                                                                                  Filter1       (q, toFunEventBoolean(cond), requirements.toSet)
    def selfJoin             (                              w1: Window, w2: Window, requirements: Requirement*): Query2[A, A] =                                                                               SelfJoin11    (q, w1, w2,                  requirements.toSet)
    def join[B]              (q2: Query1[B],                w1: Window, w2: Window, requirements: Requirement*): Query2[A, B] =                                                                               Join11        (q, q2, w1, w2,              requirements.toSet)
    def join[B, C]           (q2: Query2[B, C],             w1: Window, w2: Window, requirements: Requirement*): Query3[A, B, C] =                                                                            Join12        (q, q2, w1, w2,              requirements.toSet)
    def join[B, C, D]        (q2: Query3[B, C, D],          w1: Window, w2: Window, requirements: Requirement*): Query4[A, B, C, D] =                                                                         Join13        (q, q2, w1, w2,              requirements.toSet)
    def join[B, C, D, E]     (q2: Query4[B, C, D, E],       w1: Window, w2: Window, requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Join14        (q, q2, w1, w2,              requirements.toSet)
    def join[B, C, D, E, F]  (q2: Query5[B, C, D, E, F],    w1: Window, w2: Window, requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Join15        (q, q2, w1, w2,              requirements.toSet)
    def and[B]               (q2: Query1[B],                                        requirements: Requirement*): Query2[A, B] =                                                                               Conjunction11 (q, q2,                      requirements.toSet)
    def and[B, C]            (q2: Query2[B, C],                                     requirements: Requirement*): Query3[A, B, C] =                                                                            Conjunction12 (q, q2,                      requirements.toSet)
    def and[B, C, D]         (q2: Query3[B, C, D],                                  requirements: Requirement*): Query4[A, B, C, D] =                                                                         Conjunction13 (q, q2,                      requirements.toSet)
    def and[B, C, D, E]      (q2: Query4[B, C, D, E],                               requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Conjunction14 (q, q2,                      requirements.toSet)
    def and[B, C, D, E, F]   (q2: Query5[B, C, D, E, F],                            requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Conjunction15 (q, q2,                      requirements.toSet)
    def or[B]                (q2: Query1[B],                                        requirements: Requirement*): Query1[Either[A, B]] =                                                                       Disjunction11 (q, q2,                      requirements.toSet)
    def or[B, C]             (q2: Query2[B, C],                                     requirements: Requirement*): Query2[Either[A, B], Either[X, C]] =                                                         Disjunction12 (q, q2,                      requirements.toSet)
    def or[B, C, D]          (q2: Query3[B, C, D],                                  requirements: Requirement*): Query3[Either[A, B], Either[X, C], Either[X, D]] =                                           Disjunction13 (q, q2,                      requirements.toSet)
    def or[B, C, D, E]       (q2: Query4[B, C, D, E],                               requirements: Requirement*): Query4[Either[A, B], Either[X, C], Either[X, D], Either[X, E]] =                             Disjunction14 (q, q2,                      requirements.toSet)
    def or[B, C, D, E, F]    (q2: Query5[B, C, D, E, F],                            requirements: Requirement*): Query5[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F]] =               Disjunction15 (q, q2,                      requirements.toSet)
    def or[B, C, D, E, F, G] (q2: Query6[B, C, D, E, F, G],                         requirements: Requirement*): Query6[Either[A, B], Either[X, C], Either[X, D], Either[X, E], Either[X, F], Either[X, G]] = Disjunction16 (q, q2,                      requirements.toSet)
  }

  case class Query2Helper[A, B](q: Query2[A, B]) {
    def where                (                              cond: (A, B) => Boolean, requirements: Requirement*): Query2[A, B] =                                                                               Filter2       (q, toFunEventBoolean(cond), requirements.toSet)
    def dropElem1            (                                                       requirements: Requirement*): Query1[B] =                                                                                  DropElem1Of2  (q,                          requirements.toSet)
    def dropElem2            (                                                       requirements: Requirement*): Query1[A] =                                                                                  DropElem2Of2  (q,                          requirements.toSet)
    def selfJoin             (                              w1: Window, w2: Window,  requirements: Requirement*): Query4[A, B, A, B] =                                                                         SelfJoin22    (q, w1, w2,                  requirements.toSet)
    def join[C]              (q2: Query1[C],                w1: Window, w2: Window,  requirements: Requirement*): Query3[A, B, C] =                                                                            Join21        (q, q2, w1, w2,              requirements.toSet)
    def join[C, D]           (q2: Query2[C, D],             w1: Window, w2: Window,  requirements: Requirement*): Query4[A, B, C, D] =                                                                         Join22        (q, q2, w1, w2,              requirements.toSet)
    def join[C, D, E]        (q2: Query3[C, D, E],          w1: Window, w2: Window,  requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Join23        (q, q2, w1, w2,              requirements.toSet)
    def join[C, D, E, F]     (q2: Query4[C, D, E, F],       w1: Window, w2: Window,  requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Join24        (q, q2, w1, w2,              requirements.toSet)
    def and[C]               (q2: Query1[C],                                         requirements: Requirement*): Query3[A, B, C] =                                                                            Conjunction21 (q, q2,                      requirements.toSet)
    def and[C, D]            (q2: Query2[C, D],                                      requirements: Requirement*): Query4[A, B, C, D] =                                                                         Conjunction22 (q, q2,                      requirements.toSet)
    def and[C, D, E]         (q2: Query3[C, D, E],                                   requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Conjunction23 (q, q2,                      requirements.toSet)
    def and[C, D, E, F]      (q2: Query4[C, D, E, F],                                requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Conjunction24 (q, q2,                      requirements.toSet)
    def or[C]                (q2: Query1[C],                                         requirements: Requirement*): Query2[Either[A, C], Either[B, X]] =                                                         Disjunction21 (q, q2,                      requirements.toSet)
    def or[C, D]             (q2: Query2[C, D],                                      requirements: Requirement*): Query2[Either[A, C], Either[B, D]] =                                                         Disjunction22 (q, q2,                      requirements.toSet)
    def or[C, D, E]          (q2: Query3[C, D, E],                                   requirements: Requirement*): Query3[Either[A, C], Either[B, D], Either[X, E]] =                                           Disjunction23 (q, q2,                      requirements.toSet)
    def or[C, D, E, F]       (q2: Query4[C, D, E, F],                                requirements: Requirement*): Query4[Either[A, C], Either[B, D], Either[X, E], Either[X, F]] =                             Disjunction24 (q, q2,                      requirements.toSet)
    def or[C, D, E, F, G]    (q2: Query5[C, D, E, F, G],                             requirements: Requirement*): Query5[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G]] =               Disjunction25 (q, q2,                      requirements.toSet)
    def or[C, D, E, F, G, H] (q2: Query6[C, D, E, F, G, H],                          requirements: Requirement*): Query6[Either[A, C], Either[B, D], Either[X, E], Either[X, F], Either[X, G], Either[X, H]] = Disjunction26 (q, q2,                      requirements.toSet)
  }

  case class Query3Helper[A, B, C](q: Query3[A, B, C]) {
    def where                (                              cond: (A, B, C) => Boolean, requirements: Requirement*): Query3[A, B, C] =                                                                            Filter3       (q, toFunEventBoolean(cond), requirements.toSet)
    def dropElem1            (                                                          requirements: Requirement*): Query2[B, C] =                                                                               DropElem1Of3  (q,                          requirements.toSet)
    def dropElem2            (                                                          requirements: Requirement*): Query2[A, C] =                                                                               DropElem2Of3  (q,                          requirements.toSet)
    def dropElem3            (                                                          requirements: Requirement*): Query2[A, B] =                                                                               DropElem3Of3  (q,                          requirements.toSet)
    def selfJoin             (                              w1: Window, w2: Window,     requirements: Requirement*): Query6[A, B, C, A, B, C] =                                                                   SelfJoin33    (q, w1, w2,                  requirements.toSet)
    def join[D]              (q2: Query1[D],                w1: Window, w2: Window,     requirements: Requirement*): Query4[A, B, C, D] =                                                                         Join31        (q, q2, w1, w2,              requirements.toSet)
    def join[D, E]           (q2: Query2[D, E],             w1: Window, w2: Window,     requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Join32        (q, q2, w1, w2,              requirements.toSet)
    def join[D, E, F]        (q2: Query3[D, E, F],          w1: Window, w2: Window,     requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Join33        (q, q2, w1, w2,              requirements.toSet)
    def and[D]               (q2: Query1[D],                                            requirements: Requirement*): Query4[A, B, C, D] =                                                                         Conjunction31 (q, q2,                      requirements.toSet)
    def and[D, E]            (q2: Query2[D, E],                                         requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Conjunction32 (q, q2,                      requirements.toSet)
    def and[D, E, F]         (q2: Query3[D, E, F],                                      requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Conjunction33 (q, q2,                      requirements.toSet)
    def or[D]                (q2: Query1[D],                                            requirements: Requirement*): Query3[Either[A, D], Either[B, X], Either[C, X]] =                                           Disjunction31 (q, q2,                      requirements.toSet)
    def or[D, E]             (q2: Query2[D, E],                                         requirements: Requirement*): Query3[Either[A, D], Either[B, E], Either[C, X]] =                                           Disjunction32 (q, q2,                      requirements.toSet)
    def or[D, E, F]          (q2: Query3[D, E, F],                                      requirements: Requirement*): Query3[Either[A, D], Either[B, E], Either[C, F]] =                                           Disjunction33 (q, q2,                      requirements.toSet)
    def or[D, E, F, G]       (q2: Query4[D, E, F, G],                                   requirements: Requirement*): Query4[Either[A, D], Either[B, E], Either[C, F], Either[X, G]] =                             Disjunction34 (q, q2,                      requirements.toSet)
    def or[D, E, F, G, H]    (q2: Query5[D, E, F, G, H],                                requirements: Requirement*): Query5[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H]] =               Disjunction35 (q, q2,                      requirements.toSet)
    def or[D, E, F, G, H, I] (q2: Query6[D, E, F, G, H, I],                             requirements: Requirement*): Query6[Either[A, D], Either[B, E], Either[C, F], Either[X, G], Either[X, H], Either[X, I]] = Disjunction36 (q, q2,                      requirements.toSet)
  }

  case class Query4Helper[A, B, C, D](q: Query4[A, B, C, D]) {
    def where                (                              cond: (A, B, C, D) => Boolean, requirements: Requirement*): Query4[A, B, C, D] =                                                                         Filter4       (q, toFunEventBoolean(cond), requirements.toSet)
    def dropElem1            (                                                             requirements: Requirement*): Query3[B, C, D] =                                                                            DropElem1Of4  (q,                          requirements.toSet)
    def dropElem2            (                                                             requirements: Requirement*): Query3[A, C, D] =                                                                            DropElem2Of4  (q,                          requirements.toSet)
    def dropElem3            (                                                             requirements: Requirement*): Query3[A, B, D] =                                                                            DropElem3Of4  (q,                          requirements.toSet)
    def dropElem4            (                                                             requirements: Requirement*): Query3[A, B, C] =                                                                            DropElem4Of4  (q,                          requirements.toSet)
    def join[E]              (q2: Query1[E],                w1: Window, w2: Window,        requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Join41        (q, q2, w1, w2,              requirements.toSet)
    def join[E, F]           (q2: Query2[E, F],             w1: Window, w2: Window,        requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Join42        (q, q2, w1, w2,              requirements.toSet)
    def and[E]               (q2: Query1[E],                                               requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Conjunction41 (q, q2,                      requirements.toSet)
    def and[E, F]            (q2: Query2[E, F],                                            requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Conjunction42 (q, q2,                      requirements.toSet)
    def or[E]                (q2: Query1[E],                                               requirements: Requirement*): Query4[Either[A, E], Either[B, X], Either[C, X], Either[D, X]] =                             Disjunction41 (q, q2,                      requirements.toSet)
    def or[E, F]             (q2: Query2[E, F],                                            requirements: Requirement*): Query4[Either[A, E], Either[B, F], Either[C, X], Either[D, X]] =                             Disjunction42 (q, q2,                      requirements.toSet)
    def or[E, F, G]          (q2: Query3[E, F, G],                                         requirements: Requirement*): Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, X]] =                             Disjunction43 (q, q2,                      requirements.toSet)
    def or[E, F, G, H]       (q2: Query4[E, F, G, H],                                      requirements: Requirement*): Query4[Either[A, E], Either[B, F], Either[C, G], Either[D, H]] =                             Disjunction44 (q, q2,                      requirements.toSet)
    def or[E, F, G, H, I]    (q2: Query5[E, F, G, H, I],                                   requirements: Requirement*): Query5[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I]] =               Disjunction45 (q, q2,                      requirements.toSet)
    def or[E, F, G, H, I, J] (q2: Query6[E, F, G, H, I, J],                                requirements: Requirement*): Query6[Either[A, E], Either[B, F], Either[C, G], Either[D, H], Either[X, I], Either[X, J]] = Disjunction46 (q, q2,                      requirements.toSet)
  }

  case class Query5Helper[A, B, C, D, E](q: Query5[A, B, C, D, E]) {
    def where                (                              cond: (A, B, C, D, E) => Boolean, requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      Filter5       (q, toFunEventBoolean(cond), requirements.toSet)
    def dropElem1       (                                                                requirements: Requirement*): Query4[B, C, D, E] =                                                                              DropElem1Of5  (q,                          requirements.toSet)
    def dropElem2       (                                                                requirements: Requirement*): Query4[A, C, D, E] =                                                                              DropElem2Of5  (q,                          requirements.toSet)
    def dropElem3       (                                                                requirements: Requirement*): Query4[A, B, D, E] =                                                                              DropElem3Of5  (q,                          requirements.toSet)
    def dropElem4       (                                                                requirements: Requirement*): Query4[A, B, C, E] =                                                                              DropElem4Of5  (q,                          requirements.toSet)
    def dropElem5       (                                                                requirements: Requirement*): Query4[A, B, C, D] =                                                                              DropElem5Of5  (q,                          requirements.toSet)
    def join[F]              (q2: Query1[F],                w1: Window, w2: Window,           requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Join51        (q, q2, w1, w2,              requirements.toSet)
    def and[F]               (q2: Query1[F],                                                  requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Conjunction51 (q, q2,                      requirements.toSet)
    def or[F]                (q2: Query1[F],                                                  requirements: Requirement*): Query5[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X]] =               Disjunction51 (q, q2,                      requirements.toSet)
    def or[F, G]             (q2: Query2[F, G],                                               requirements: Requirement*): Query5[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X]] =               Disjunction52 (q, q2,                      requirements.toSet)
    def or[F, G, H]          (q2: Query3[F, G, H],                                            requirements: Requirement*): Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X]] =               Disjunction53 (q, q2,                      requirements.toSet)
    def or[F, G, H, I]       (q2: Query4[F, G, H, I],                                         requirements: Requirement*): Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X]] =               Disjunction54 (q, q2,                      requirements.toSet)
    def or[F, G, H, I, J]    (q2: Query5[F, G, H, I, J],                                      requirements: Requirement*): Query5[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J]] =               Disjunction55 (q, q2,                      requirements.toSet)
    def or[F, G, H, I, J, K] (q2: Query6[F, G, H, I, J, K],                                   requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[X, K]] = Disjunction56 (q, q2,                      requirements.toSet)
  }

  case class Query6Helper[A, B, C, D, E, F](q: Query6[A, B, C, D, E, F]) {
    def where                (                              cond: (A, B, C, D, E, F) => Boolean, requirements: Requirement*): Query6[A, B, C, D, E, F] =                                                                   Filter6           (q, toFunEventBoolean(cond), requirements.toSet)
    def dropElem1            (                                                                   requirements: Requirement*): Query5[B, C, D, E, F] =                                                                      DropElem1Of6      (q,                          requirements.toSet)
    def dropElem2            (                                                                   requirements: Requirement*): Query5[A, C, D, E, F] =                                                                      DropElem2Of6      (q,                          requirements.toSet)
    def dropElem3            (                                                                   requirements: Requirement*): Query5[A, B, D, E, F] =                                                                      DropElem3Of6      (q,                          requirements.toSet)
    def dropElem4            (                                                                   requirements: Requirement*): Query5[A, B, C, E, F] =                                                                      DropElem4Of6      (q,                          requirements.toSet)
    def dropElem5            (                                                                   requirements: Requirement*): Query5[A, B, C, D, F] =                                                                      DropElem5Of6      (q,                          requirements.toSet)
    def dropElem6            (                                                                   requirements: Requirement*): Query5[A, B, C, D, E] =                                                                      DropElem6Of6      (q,                          requirements.toSet)
    def or[G]                (q2: Query1[G],                                                     requirements: Requirement*): Query6[Either[A, F], Either[B, X], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] = Disjunction61     (q, q2,                      requirements.toSet)
    def or[G, H]             (q2: Query2[G, H],                                                  requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, X], Either[D, X], Either[E, X], Either[F, X]] = Disjunction62     (q, q2,                      requirements.toSet)
    def or[G, H, I]          (q2: Query3[G, H, I],                                               requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, X], Either[E, X], Either[F, X]] = Disjunction63     (q, q2,                      requirements.toSet)
    def or[G, H, I, J]       (q2: Query4[G, H, I, J],                                            requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, X], Either[F, X]] = Disjunction64     (q, q2,                      requirements.toSet)
    def or[G, H, I, J, K]    (q2: Query5[G, H, I, J, K],                                         requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, X]] = Disjunction65     (q, q2,                      requirements.toSet)
    def or[G, H, I, J, K, L] (q2: Query6[G, H, I, J, K, L],                                      requirements: Requirement*): Query6[Either[A, F], Either[B, G], Either[C, H], Either[D, I], Either[E, J], Either[F, K]] = Disjunction66     (q, q2,                      requirements.toSet)
  }

  implicit def query1ToQuery1Helper[A]                (q: Query1[A]):                Query1Helper[A] =                Query1Helper (q)
  implicit def query2ToQuery2Helper[A, B]             (q: Query2[A, B]):             Query2Helper[A, B] =             Query2Helper (q)
  implicit def query3ToQuery3Helper[A, B, C]          (q: Query3[A, B, C]):          Query3Helper[A, B, C] =          Query3Helper (q)
  implicit def query4ToQuery4Helper[A, B, C, D]       (q: Query4[A, B, C, D]):       Query4Helper[A, B, C, D] =       Query4Helper (q)
  implicit def query5ToQuery5Helper[A, B, C, D, E]    (q: Query5[A, B, C, D, E]):    Query5Helper[A, B, C, D, E] =    Query5Helper (q)
  implicit def query6ToQuery6Helper[A, B, C, D, E, F] (q: Query6[A, B, C ,D, E, F]): Query6Helper[A, B, C, D, E, F] = Query6Helper (q)

}
