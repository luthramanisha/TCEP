package tcep.machinenodes

import tcep.dsl.Dsl.{Frequency, _}
import tcep.placement.benchmarking.BenchmarkingNode

/**
  * Created by mac on 03/11/2017.
  */
object TestApp extends App {

  val freqReq = frequency > Frequency(50, 5) otherwise Option.empty
  val latencyRequirement = latency < timespan(500.milliseconds) otherwise Option.empty
  val messageOverheadRequirement = hops < 10 otherwise Option.empty

  val res = BenchmarkingNode.selectBestPlacementAlgorithm(List.empty, List(freqReq))
  println(res)
}

