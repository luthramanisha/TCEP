package tcep.graph.transition.mapek.lightweight

import tcep.graph.transition.ExecutorComponent

class LightweightExecutor(mapek: LightweightMAPEK) extends ExecutorComponent(mapek){

/*  override def receive: Receive = {
    case ExecuteTransition(placementStrategy) =>
      //log.info(s"Transition execution message to $placementStrategy received!")
      placementStrategy match {
        case "Relaxation" => executeTransition(PietzuchAlgorithm)
        case "MDCEP" => executeTransition(StarksAlgorithm)
        case "Rizou" => executeTransition(RizouAlgorithm)
        case "Random" => executeTransition(RandomAlgorithm)
        case "ProducerConsumer" => executeTransition(MobilityTolerantAlgorithm)
        case "GlobalOptimalBDP" => executeTransition(GlobalOptimalBDPAlgorithm)
        case other => log.info(s"config contained unknown placement algorithm $other")
      }
  }

  def executeTransition(placement: PlacementStrategy): Unit = {
    mapek.knowledge ! SetPlacementStrategy(placement)
    for {
      client<- (mapek.knowledge ? GetClient).mapTo[ActorRef]
      mode <- (mapek.knowledge ? GetTransitionMode).mapTo[Mode]
    } yield {
      log.info(s"executing $mode transition to ${placement.name}")
      client ! TransitionRequest(placement)
    }
  }*/
}
/*object LightweightExecutor{
  case class ExecuteTransition(placementStrategy: String)
}*/