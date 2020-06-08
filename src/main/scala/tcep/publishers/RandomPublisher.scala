package tcep.publishers

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import tcep.data.Events._

import scala.concurrent.duration.FiniteDuration
import scala.util.Random

case class RandomPublisher(createEventFromId: Integer => Event) extends Publisher {

  val publisherName: String = self.path.name
  val minimumWait = 2000
  val id: AtomicInteger = new AtomicInteger(0)
  val scheduler =  context.system.scheduler.schedule(
                            FiniteDuration(minimumWait + Random.nextInt(5000), TimeUnit.MILLISECONDS),
                            FiniteDuration(minimumWait + Random.nextInt(5000), TimeUnit.MILLISECONDS),
                            runnable = () => {
                              val event: Event = createEventFromId(id.incrementAndGet())
                              subscribers.foreach(_ ! event)
                              log.info(s"STREAM $publisherName:\t$event")
                            }
                          )

  override def postStop(): Unit = {
    scheduler.cancel()
    super.postStop()
  }
}
