package org.discovery.vivaldi

import org.scalatest.FunSuite

/**
  * Created by raheel on 31/01/2018.
  */
class VivaldiPositionTest extends FunSuite{

  test("Nodes should be able to obtain their coordinates") {
    var node1 = VivaldiPosition.create()
    var node2 = VivaldiPosition.create()
    var node3 = VivaldiPosition.create()

    node1.update(15, node2.coordinates, node2.localConfidence)
    node2.update(20, node1.coordinates, node2.localConfidence)

    print(node1.coordinates)
    print(node2.coordinates)

    assert(!node1.coordinates.equals(Coordinates.origin))
    assert(!node2.coordinates.equals(Coordinates.origin))
  }
}
