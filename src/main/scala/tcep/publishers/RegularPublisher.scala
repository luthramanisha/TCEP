package tcep.publishers

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}

import tcep.data.Events._
import tcep.publishers.Publisher.StartStreams
import tcep.utils.SpecialStats

object RegularPublisher {
  case object SendEventTickKey
  case object SendEventTick
  case object LogEventsSentTickKey
  case object LogEventsSentTick
}
/**
  * Publishes the events at regular interval
  *
  * @param waitTime the interval for publishing the events in MICROSECONDS
  * @param createEventFromId function to convert Id to an event
  */
case class RegularPublisher(waitTime: Long, createEventFromId: Integer => Event) extends Publisher {
  import RegularPublisher._
  val publisherName: String = self.path.name
  val id: AtomicInteger = new AtomicInteger(0)
  var startedPublishing = false
  var emitEventTask: ScheduledFuture[_] = _
  var logTask: ScheduledFuture[_] = _
  var sched = Executors.newSingleThreadScheduledExecutor()

  override def preStart() = {
    log.info(s"stating regular publisher with roles ${cluster.getSelfRoles}")
    super.preStart()
  }
  override def postStop(): Unit = {
    emitEventTask.cancel(true)
    super.postStop()
  }

  override def receive: Receive = {
    super.receive orElse {
      case StartStreams() =>
        if (!startedPublishing) {
          log.info("starting to stream events!")
          startedPublishing = true
          //timers.startTimerAtFixedRate(SendEventTickKey, SendEventTick, waitTime.micros)
          emitEventTask = sched.scheduleAtFixedRate(() => self ! SendEventTick, 10, waitTime, TimeUnit.MICROSECONDS)
          logTask = sched.scheduleWithFixedDelay(() => self !  LogEventsSentTick, 5, 5, TimeUnit.SECONDS)
        }

      case SendEventTick =>
        val event: Event = createEventFromId(id.incrementAndGet())
        event.init()(cluster.selfAddress)
        subscribers.foreach(_ ! event)

      case LogEventsSentTick => SpecialStats.log(self.toString(), "eventsSent", s"total: ${id.get()}")

    }
  }
}
