/*
package tcep.simulation.tcep

import java.io.File

import akka.actor.{ActorContext, ActorRef}
import tcep.data.Queries.Query

class ChangingEnvSimulation(name: String, directory: Option[File], query: Query, publishers: Map[String, ActorRef],
                            recordAverageLoad: RecordAverageLoad,
                            recordLatency: RecordLatency,
                            recordMessageOverhead: RecordMessageHops,
                            recordFrequency: RecordFrequency,
                            recordOverhead: RecordOverhead,
                            recordNetworkUsage: RecordNetworkUsage,
                            recordTransitionStatus: Option[RecordTransitionStatus],
                            context: ActorContext)
                            extends Simulation (name,directory,query,publishers,recordAverageLoad,
                                               recordLatency,recordMessageOverhead,recordFrequency,recordOverhead, recordNetworkUsage,
                                                recordTransitionStatus,
                                               context){

  private val res : StringBuilder = new StringBuilder()
  override def initCSVWriter(startTime: Int, interval: Int, totalTime: Int, recordLatency: RecordLatency, callback: () => Any) = {
    var time = 0

    def createCSVEntry(): Unit = synchronized {
      if (recordLatency.lastMeasurement.isDefined
        && recordMessageOverhead.lastMeasurement.isDefined
        && recordAverageLoad.lastLoadMeasurement.isDefined
        && recordFrequency.lastMeasurement.isDefined
      ) {
        val duplicates = queryGraph.mapek.knowledge.duplicateOperators()
        val status = queryGraph.mapek.knowledge.transitionStatus
        out.append(s"$time,${recordLatency.lastMeasurement.get.toMillis},${recordMessageOverhead.lastMeasurement.get},${recordAverageLoad.lastLoadMeasurement.get.value},${recordFrequency.lastMeasurement.get},$status, ${recordOverhead.lastOverheadMeasurement.get}, ${recordNetworkUsage.lastUsageMeasurement.get}, $duplicates")
        out.println()
        time += interval
      } else {
        log.info(s"Data not available yet! ${recordLatency.lastMeasurement} ${recordMessageOverhead.lastMeasurement} ${recordAverageLoad.lastLoadMeasurement} ${recordFrequency.lastMeasurement}")
      }
    }

    val simulation: Cancellable = context.system.scheduler.schedule(FiniteDuration.apply(startTime, TimeUnit.SECONDS),
      FiniteDuration.apply(interval, TimeUnit.SECONDS
      ))(createCSVEntry())

    def stopSimulation(): Unit = {
      simulation.cancel()
      out.close()
      queryGraph.stop()
      Thread.sleep(10000) //wait for all actors to stop
      callback.apply()
    }
    context.system.scheduler.scheduleOnce(FiniteDuration.apply(totalTime, TimeUnit.SECONDS))(stopSimulation())
  }
}
*/
