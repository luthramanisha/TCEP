package tcep.placement

import tcep.placement.manets.StarksAlgorithm

/**
  * Created by raheel arif
  * on 17/08/2017.
  */
object TransitionManager {

  def getPlacementStrategy(): PlacementStrategy = StarksAlgorithm
}
