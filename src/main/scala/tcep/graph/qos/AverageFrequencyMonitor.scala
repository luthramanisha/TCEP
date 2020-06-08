package tcep.graph.qos

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import org.slf4j.LoggerFactory
import tcep.data.Events._
import tcep.data.Queries._
import tcep.dsl.Dsl.FrequencyMeasurement

import scala.collection.immutable.Queue

/**
  * Measures the frequency of messages in the interval
  */
case class AverageFrequencyMonitor(query: Query, record: Option[FrequencyMeasurement]) extends Monitor {
  val log = LoggerFactory.getLogger(getClass)
  var frequencyRequirement: Option[FrequencyRequirement] = query.requirements.collect { case lr: FrequencyRequirement => lr }.headOption
  val interval = if(frequencyRequirement.isDefined) frequencyRequirement.get.frequency.interval else 1
  implicit def queue2finitequeue[A](q: Queue[A]) = new FiniteQueue[A](q)
  var messages = Queue[Long]()

  @volatile
  var eventEmittedInInterval : AtomicInteger = new AtomicInteger(0)

  override def onEventEmit(event: Event, status: Int): X = {
    //if(!messages.contains(event.monitoringData.createTime)){
    //  messages.enqueueFinite(event.monitoringData.createTime, 3000)
      eventEmittedInInterval.incrementAndGet()
    //}
  }

  val ex = new ScheduledThreadPoolExecutor(1)
  val task = new Runnable {
    def run() = {
      if (frequencyRequirement.isDefined && frequencyRequirement.get.otherwise.isDefined) {
        frequencyRequirement.get.operator match {
          case Equal =>        if (!(eventEmittedInInterval.get() == frequencyRequirement.get.frequency.frequency)) frequencyRequirement.get.otherwise.get.apply(eventEmittedInInterval.get())
          case NotEqual =>     if (!(eventEmittedInInterval.get() != frequencyRequirement.get.frequency.frequency)) frequencyRequirement.get.otherwise.get.apply(eventEmittedInInterval.get())
          case Greater =>      if (!(eventEmittedInInterval.get() >  frequencyRequirement.get.frequency.frequency)) frequencyRequirement.get.otherwise.get.apply(eventEmittedInInterval.get())
          case GreaterEqual => if (!(eventEmittedInInterval.get() >= frequencyRequirement.get.frequency.frequency)) frequencyRequirement.get.otherwise.get.apply(eventEmittedInInterval.get())
          case Smaller =>      if (!(eventEmittedInInterval.get() <  frequencyRequirement.get.frequency.frequency)) frequencyRequirement.get.otherwise.get.apply(eventEmittedInInterval.get())
          case SmallerEqual => if (!(eventEmittedInInterval.get() <= frequencyRequirement.get.frequency.frequency)) frequencyRequirement.get.otherwise.get.apply(eventEmittedInInterval.get())
        }
      }
      log.debug(s"running frequencyMonitor task, record is defined: ${record.isDefined}, events in last ${interval} seconds: ${eventEmittedInInterval.get}")
      if(record.isDefined){
        record.get.apply(eventEmittedInInterval.get())
      }
      eventEmittedInInterval.set(0)
    }
  }

  ex.scheduleAtFixedRate(task, interval, interval, TimeUnit.SECONDS)


  class FiniteQueue[A](q: Queue[A]) {
    def enqueueFinite[B >: A](elem: B, maxSize: Int): Queue[B] = {
      var ret = q.enqueue(elem)
      while (ret.size > maxSize) { ret = ret.dequeue._2 }
      ret
    }
  }

}

/**
  * Creates AverageFrequencyMonitor
  * @param query CEP query
  * @param record callback for the udpated values of frequency per interval
  */
case class AverageFrequencyMonitorFactory(query: Query, record: Option[FrequencyMeasurement]) extends MonitorFactory {
  override def createNodeMonitor: Monitor = AverageFrequencyMonitor(query,record)
}