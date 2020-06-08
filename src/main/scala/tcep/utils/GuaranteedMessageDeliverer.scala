package tcep.utils

import java.util.concurrent.Executors

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import tcep.machinenodes.helper.actors.{Message, TransitionControlMessage}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

case class CriticalMessage(receiver: ActorRef, msg: Message) extends TransitionControlMessage
case class NACK() extends TransitionControlMessage

class GuaranteedMessageDeliverer[R <: Message](tlf: Option[(String, ActorRef) => Unit] = None, tlp: Option[ActorRef] = None,
                                               retryTimeout: Timeout,
                                               var sendTime: Long = System.currentTimeMillis())
  //extends PersistentActor  with AtLeastOnceDelivery
   extends Actor with ActorLogging {
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))
  var resendTask: Cancellable = null
  var s: ActorRef = _

  override def postStop(): Unit = {
    if(resendTask != null) resendTask.cancel()
    super.postStop()
  }

  override def receive: Receive = {

    case m: CriticalMessage =>
      s = sender() // usually tmp actor
      sendTime = System.currentTimeMillis()
      val logMsg = s"GUARANTEED_DELIVERY delivering $m for $s until response"
      if(tlf.isDefined && tlp.isDefined) tlf.get(logMsg, tlp.get)
      else log.info(logMsg)
      m.receiver ! m.msg
      resendTask = context.system.scheduler.schedule(retryTimeout.duration, retryTimeout.duration)(resend(m.msg, m.receiver)) // cover loss of msg itself, expects receiver to send at least one ACK

    case response: R =>
      val logMsg = s"GUARANTEED_DELIVERY for $s successful after ${System.currentTimeMillis() - sendTime}, response received from ${sender()}"
      if(tlf.isDefined && tlp.isDefined) tlf.get(logMsg, tlp.get)
      else log.info(logMsg)
      resendTask.cancel()
      s ! response

    case other =>
      val logMsg = s"GUARANTEED_DELIVERY unknown message ${other.getClass} from ${sender()}"
      if(tlf.isDefined && tlp.isDefined) tlf.get(logMsg, tlp.get)
      else log.info(logMsg)
  }

  def resend(msg: Message, receiver: ActorRef) = {
    receiver.tell(msg, self)
    val logMsg = s"failed to receive ACK from $receiver within $retryTimeout for $msg, resending now".toUpperCase()
    if(tlf.isDefined && tlp.isDefined) tlf.get(logMsg, tlp.get)
    else log.info(logMsg)
  }
}
