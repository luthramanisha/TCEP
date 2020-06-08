package tcep.graph.qos

import java.io.{File, PrintStream}

import org.slf4j.LoggerFactory
import tcep.data.Events.{Event, Event2}
import tcep.data.Queries._

case class MessageMonitor(csvWriter: PrintStream) extends Monitor {
  val log = LoggerFactory.getLogger(getClass)

  override def onEventEmit(event: Event, transitionStatus: Int): X = {
    // log.info(s"Message emitted ${event}")
    val event2: Event2 = event.asInstanceOf[Event2]
    csvWriter.println(s"${event2.e1} \t ${event2.e2}")
  }
}

case class MessageMonitorFactory(query : Query, directory: Option[File]) extends MonitorFactory {

  val csvWriter = directory map { directory => new PrintStream(new File(directory, s"messages.csv"))
  } getOrElse java.lang.System.out

  override def createNodeMonitor: Monitor = MessageMonitor(csvWriter)
}
