package tcep.placement.benchmarking

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorLogging, Cancellable}
import akka.cluster.ClusterEvent.{MemberJoined, MemberLeft}
import tcep.ClusterActor

import scala.concurrent.duration._

/**
  * Created by Raheel
  * on 21/09/2017.
  */
class NetworkStateMonitorNode extends ClusterActor with ActorLogging {

  var updateStateScheduler: Cancellable = _
  val churnRateThreshold = 5
  var changeRate: AtomicInteger = new AtomicInteger(0)
  var networkChurnRate: NetworkChurnRate = LowChurnRate //default value

  override def preStart(): Unit = {
    super.preStart()
    updateStateScheduler = this.context.system.scheduler.schedule(5 minutes, 5 minutes, this.self, UpdateState)
  }

  override def receive: Receive = {
    case MemberJoined(member) => {
      log.info(s"new member joined $member")
      changeRate.incrementAndGet()
    }

    case MemberLeft(member) => {
      changeRate.incrementAndGet()
    }
    case UpdateState => updateState()
  }

  def updateState(): Unit = {
    if (changeRate.intValue() > churnRateThreshold) {
      log.info("Hight churn rate of the system")
      networkChurnRate = HighChurnRate
      changeRate.set(0)
    } else {
      log.info("Low churn rate of the system")
      networkChurnRate = LowChurnRate
      changeRate.set(0)
    }

  }
}

case object UpdateState
case object RegisterForStateUpdate
case object CancelRegistration

abstract class NetworkChurnRate()
case object HighChurnRate extends NetworkChurnRate
case object LowChurnRate extends NetworkChurnRate
