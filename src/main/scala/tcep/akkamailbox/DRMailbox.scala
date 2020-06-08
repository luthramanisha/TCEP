package tcep.akkamailbox

import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.{ActorPath, ActorRef, ActorSystem}
import akka.dispatch._
import com.typesafe.config.Config

import scala.collection.mutable

/**
  * Created by raheel
  * on 07/08/2017.
  */


object MessageQueue {
  val mailboxMap = new mutable.HashMap[ActorPath, MessageQueue]()

  def dataRate(receiver: ActorPath, sender: ActorPath): Double = {
    if (!mailboxMap.contains(receiver))
      0d
    else {
      val messageHistory = mailboxMap(receiver).rateMap.get(sender)
      if (messageHistory.isEmpty) return 0d
      val timeSpan = (System.currentTimeMillis() - messageHistory.get.firstMessage) * 0.001
      messageHistory.get.messagesReceived / timeSpan
    }
  }
}

class MessageQueue extends ConcurrentLinkedQueue[Envelope] with UnboundedQueueBasedMessageQueue {
  final def queue: util.Queue[Envelope] = this

  //keeps track number of bytes received from each sender
  //data format: ACTOR_PATH, TIMESTAMP_OF_FIRST_MESSAGE, BYTES_RECEIVED
  val rateMap = new mutable.HashMap[ActorPath, MessageHistory]

  override def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
    super.enqueue(receiver, handle)
    updateRateMap(handle.sender, handle.message)

    if (!MessageQueue.mailboxMap.contains(receiver.path)) {
      MessageQueue.mailboxMap.put(receiver.path, this)
    }
  }

  def updateRateMap(sender: ActorRef, message: Any): Unit = {
    if (rateMap.contains(sender.path)) {
      val originalVal = rateMap(sender.path)
      rateMap.update(sender.path, MessageHistory(originalVal.firstMessage, originalVal.messagesReceived + 1))
    } else {
      rateMap.put(sender.path, MessageHistory(System.currentTimeMillis(), 1))
    }
  }
}

// DRMailbox keeps track of message rate from different senders
class DRMailbox extends MailboxType with ProducesMessageQueue[MessageQueue] {
  def this(settings: ActorSystem.Settings, config: Config) = this()

  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = new MessageQueue
}

case class MessageHistory(firstMessage: Long, messagesReceived: Long) {}
