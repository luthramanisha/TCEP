package org.discovery.vivaldi

/**
  * Created by raheel on 31/01/2018.
  * Based on the client implementation of Vuze JS library
  */
class VivaldiPosition(var coordinates: Coordinates) extends Serializable {

  val CONVERGE_EVERY = 5
  val CONVERGE_FACTOR = 50
  val ERROR_MIN = 0.1

  val cc = 0.25
  val ce = 0.5

  val initialConfidence: Double = 10 // moving average, relative error: (c1 - c2) / rtt(c1, c2)
  var localConfidence: Double = initialConfidence
  var totalUpdates = 0

  def getCoordinates(): Coordinates = {
    this.coordinates
  }

  def getLocation(): (Double, Double) = {
    this.coordinates.getCoordinates()
  }

  def getErrorEstimate(): Double = {
    this.localConfidence
  }

  def setErrorEstimate(e: Double) {
    this.localConfidence = e
  }

  def isValid(): Boolean = {
    this.getCoordinates().isValid()
  }

  def estimateRTT(data: Coordinates): Double = {
    this.coordinates.distance(data)
  }

  def estimateRTT(data: VivaldiPosition) = {
    val coords = data.getCoordinates()

    if (coords.atOrigin() || this.coordinates.atOrigin()) {
      throw new RuntimeException("invalid state")
    }

    this.coordinates.distance(coords)
  }


  def equals(other: VivaldiPosition): Boolean = {
    var res = true
    if (other.localConfidence != this.localConfidence) {
      res = false
    }

    if (!other.coordinates.equals(this.coordinates)) {
      res = false
    }
    res
  }

  def update(rtt: Double, other: Coordinates, otherConfidence: Double): Boolean = {

    //println(s"updating coordinates ${coordinates} with rtt of $rtt to other coordinates $other ")
    if (!other.isValid()) {
      throw new RuntimeException(s"received invalid other coordinates: ${other}")
    }

    val confidence = this.localConfidence

    // Ensure we have valid data in input
    // (clock changes lead to crazy rtt values)
    if (rtt <= 0 || rtt > 5 * 60 * 1000)
      throw new RuntimeException(s"received invalid ping value for update: ${rtt}")

    if (confidence + otherConfidence <= 0)
      throw new RuntimeException(s"confidence of both coordinates is zero ${confidence + otherConfidence}")

    // Sample weight balances local and remote error. (1)
    val w = confidence / (otherConfidence + confidence)

    // Real error
    val re = rtt - this.coordinates.distance(other)

    // Compute relative error of this sample. (2)
    val es = Math.abs(re) / rtt

    // Update weighted moving average of local error. (3)
    val new_error = es * ce * w + confidence * (1 - ce * w)

    // Update local coordinates. (4)
    val delta = cc * w
    val scale = delta * re

    val random_error = new Coordinates(Math.random() / 10, Math.random() / 10, 0)
    val new_coordinates = this.coordinates.add(this.coordinates.sub(other.add(random_error)).unity().scale(scale))

    if (new_coordinates.isValid()) {
      this.coordinates = new_coordinates
      this.localConfidence = if (new_error > ERROR_MIN) new_error else ERROR_MIN
    } else {
      this.coordinates = new Coordinates(0, 0, 0)
      this.localConfidence = initialConfidence
    }

    if (!other.atOrigin()) {
      this.totalUpdates = totalUpdates + 1
    }
    if (this.totalUpdates > CONVERGE_EVERY) { // this acts as gravity-like force to keep coordinates from drifting away from origin
      this.totalUpdates = 0
      this.update(10, new Coordinates(0, 0, 0), CONVERGE_FACTOR)
    }

    true
  }

}

object VivaldiPosition {
  def create(): VivaldiPosition = {
    new VivaldiPosition(new Coordinates(0, 0, 0))
  }
}