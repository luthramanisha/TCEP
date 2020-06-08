package tcep.simulation.adaptive.cep

import akka.actor.{ActorSystem, Props}

object Main extends App {
  val actorSystem: ActorSystem = ActorSystem()
  actorSystem.actorOf(Props(BaseActor()))
}
