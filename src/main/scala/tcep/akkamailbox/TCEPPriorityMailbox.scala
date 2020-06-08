package tcep.akkamailbox

import akka.actor.{ActorSystem, PoisonPill}
import akka.dispatch.{PriorityGenerator, UnboundedStablePriorityMailbox}
import com.typesafe.config.Config
import tcep.machinenodes.helper.actors.{MeasurementMessage, TransitionControlMessage, VivaldiCoordinatesMessage}

/**
  * Created by raheel on 19/01/2018.
  * updated by niels on 20/12/2018
  */
class TCEPPriorityMailbox(settings: ActorSystem.Settings, config: Config)
  extends UnboundedStablePriorityMailbox(
    PriorityGenerator {
      //lower value -> higher priority
      case c: TransitionControlMessage => 0
      case m: MeasurementMessage => 1
      case PoisonPill     => 3
      case v: VivaldiCoordinatesMessage => 4
      case otherwise      => 2
    })
