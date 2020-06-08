package tcep.publishers

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Cancellable
import tcep.data.Events.Event
import tcep.publishers.Publisher.StartStreams

import scala.concurrent.duration.FiniteDuration


case class UnregularPublisher(waitTime: Long, createEventFromId: Integer => (Event, Double), waitTillStart: Option[Long] = None) extends Publisher {

  val publisherName: String = self.path.name
  val id: AtomicInteger = new AtomicInteger(0)
  var startedPublishing = false
  var emitEventTask: Cancellable = _

  override def preStart(): Unit = {
    log.info(s"starting unregular publisher with roles ${cluster.getSelfRoles}")
    super.preStart()
  }

  override def postStop(): Unit = {
    emitEventTask.cancel()
    super.postStop()
  }

  override def receive: Receive = {
    super.receive orElse {
      case StartStreams() =>
        log.info("UNREGULAR PUBLISHER STARTING!")
        emitEventTask = context.system.scheduler.scheduleOnce(
          FiniteDuration(waitTime, TimeUnit.SECONDS))(publishEvent)
    }
  }

  def publishEvent(): Unit = {
    if (!startedPublishing) {
      log.info("Starting stream")
      startedPublishing = true
    }

    val tup: (Event, Double) = createEventFromId(id.incrementAndGet())
    val event = tup._1
    event.init()(cluster.selfAddress)
    val waittime = tup._2*1000
    log.info(s"Emitting event: $event and waiting for ${waittime.toLong}ms")
    subscribers.foreach(_ ! event)
    emitEventTask = context.system.scheduler.scheduleOnce(FiniteDuration(waittime.toLong, TimeUnit.MILLISECONDS))(publishEvent)
  }
}
