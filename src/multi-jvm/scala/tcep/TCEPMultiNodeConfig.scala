package tcep

import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory

/**
  * executes a multi-node akka cluster test by running one JVM for each node
  * run using 'sbt multi-jvm:test'
  * see https://doc.akka.io/docs/akka/2.5/multi-node-testing.html
  */
// config for test nodes of the cluster
object TCEPMultiNodeConfig extends MultiNodeConfig {

  // 'role' does not mean member role, but cluster node
  val client = role("client")
  val node1 = role("node1")
  val node2 = role("node2")
  //val node3 = role("node3")
  val publisher1 = role("publisher1")
  val publisher2 = role("publisher2")
  //val processingNodes = Seq(node1, node2, node3)
  //val publishers = Seq(publisher1, publisher2)

  // enable the test transport that allows to do fancy things such as blackhole, throttle, etc.
  testTransport(on = false)

  // configuration for client
  nodeConfig(client)(ConfigFactory.parseString(
    """
      |akka.cluster.roles=[Subscriber, Candidate]
      |akka.remote.classic.netty.tcp.port=2500
    """.stripMargin))

  nodeConfig(publisher1)(ConfigFactory.parseString(
    """
      |akka.remote.classic.netty.tcp.port=2501
      |akka.cluster.roles=[Publisher]
    """.stripMargin))

  nodeConfig(publisher2)(ConfigFactory.parseString(
    """
      |akka.remote.classic.netty.tcp.port=2502
      |akka.cluster.roles=[Publisher]
    """.stripMargin))

  nodeConfig(node1)(ConfigFactory.parseString(
    """
      |akka.remote.classic.netty.tcp.port=2503
      |akka.cluster.roles=[Candidate]
    """.stripMargin
  ))

  nodeConfig(node2)(ConfigFactory.parseString(
    """
      |akka.remote.classic.netty.tcp.port=2504
      |akka.cluster.roles=[Candidate]
    """.stripMargin
  ))
  //
  //nodeConfig(node3)(ConfigFactory.parseString(
  //  """
  //    |akka.remote.netty.tcp.port=2505
  //    |akka.cluster.roles=[Candidate]
  //  """.stripMargin
  //))
  // common configuration for all nodes
  commonConfig(ConfigFactory.parseString(
    """
      |akka.loglevel=WARNING
      |akka.actor.provider = cluster
      |clustering.cluster.name = tcep
      |akka.coordinated-shutdown.run-by-jvm-shutdown-hook = off
      |akka.coordinated-shutdown.terminate-actor-system = off
      |akka.cluster.run-coordinated-shutdown-when-down = off
      |prio-dispatcher { mailbox-type = "tcep.akkamailbox.TCEPPriorityMailbox" }
      |prio-mailbox { mailbox-type = "tcep.akkamailbox.TCEPPriorityMailbox" }
      |akka.actor.deployment { /PrioActor { mailbox = prio-mailbox } }
      |akka.actor.serializers {
      |      # Define kryo serializer
      |      kryo = "com.twitter.chill.akka.AkkaSerializer"
      |    }
      |akka.actor.serialization-bindings { "java.io.Serializable" = kryo }
    """.stripMargin))
}
