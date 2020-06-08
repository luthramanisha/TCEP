package org.discovery.vivaldi

import akka.cluster.Member

/**
  * Created by raheel on 31/01/2018.
  */
case class Coordinates(x: Double, y: Double, h: Double) {

  val MAX_X = 3000000
  val MAX_Y = 3000000
  val MAX_H = 3000000

  def add(other: Coordinates): Coordinates = {
    primitive(this, other, 1)
  }

  def sub(other: Coordinates): Coordinates = {
    primitive(this, other, -1)
  }

  def scale(scale: Double): Coordinates = {
    new Coordinates(
      scale * this.x,
      scale * this.y,
      scale * this.h
    )
  }

  def measure(): Double = {
    Math.sqrt(this.x * this.x + this.y * this.y) + this.h
  }

  def atOrigin(): Boolean = {
    this.x == 0 && this.y == 0
  }

  def isValid(): Boolean = {
    Math.abs(this.x) <= MAX_X &&
      Math.abs(this.y) <= MAX_Y &&
      Math.abs(this.h) <= MAX_H
  }

  def distance(other: Coordinates): Double = {
    this.sub(other).measure()
  }

  def distance(other: (Member, Coordinates)): Double = {
    primitive(this, other._2)
  }

  def unity(): Coordinates = {
    val measure = this.measure()
    if(measure != 0) this.scale(1 / measure)
    else this
  }

  def getCoordinates(): (Double, Double) = {
    (this.x, this.y)
  }

  def equals(other: Coordinates): Boolean = {
    if (other.x != this.x || other.y != this.y || other.h != this.h) {
      return false
    }
    true
  }

  override def toString: String = {
    s"(${BigDecimal(x).setScale(1, BigDecimal.RoundingMode.HALF_UP)}, ${BigDecimal(y).setScale(1, BigDecimal.RoundingMode.HALF_UP)})"
  }

  def primitive(c1: Coordinates, c2: Coordinates, scale: Double): Coordinates = {
    new Coordinates(
      c1.x + c2.x * scale,
      c1.y + c2.y * scale,
      Math.abs(c1.h + c2.h)
    )
  }

  def primitive(c1: Coordinates, c2: Coordinates): Double = {
    maxOrCurrent(primitive(c1, c2, 5).measure().toInt, 100).toDouble
  }

  def maxOrCurrent(current: Int, max: Int) = if (current > max) max else current

}

object Coordinates {
  def origin: Coordinates = new Coordinates(0, 0, 0)

  /**
    * checks if the given sequence of coordinates can be considered equal
    * @param coordinates
    * @return true if all distance pairs are smaller than 0.001d, else false
    */
  def areAllEqual(coordinates: Seq[Coordinates]): Boolean = {
    val distances: Seq[Seq[Double]] = coordinates.map(c => coordinates.map(other => c.distance(other)))
    return !distances.exists(c => c.exists(dist => dist > 0.001d))
  }
}