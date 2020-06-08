package tcep.simulation.tcep

import akka.actor.{ActorRef, Address}
import akka.cluster.{Cluster, Member, MemberStatus}
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.LatencyDistance
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.{Http, HttpOptions}
import tcep.placement.HostInfo

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.parsing.json.{JSONArray, JSONObject}


object GUIConnector {
  protected val log: Logger = LoggerFactory.getLogger(getClass)
  protected val guiEndpoint: String = ConfigFactory.load().getString("constants.gui-endpoint")
  private val clusterName: String = ConfigFactory.load().getString("clustering.cluster.name")
  private val publisherBasePort: Int = ConfigFactory.load().getInt("constants.base-port") + 1
  private val nSpeedPublishers: Int = ConfigFactory.load().getInt("constants.number-of-speed-publisher-nodes")
  private val isMininetSimulation: Boolean = ConfigFactory.load().getBoolean("constants.mininet-simulation")

  private def getPrimaryRole(member: Member) = {
    val primaryRole = member.roles.filterNot(r => r == "dc-default" || r == "Candidate") // filter Candidate here since publishers and subscriber are Candidate as well
    if(primaryRole.isEmpty) "Candidate" else primaryRole.head
  }

  /**
    * for mininet simulation: transform host IP to a name (the format used in the docker swarm setup)
    * @param address
    */
  private def convertAddress(address: Address)(implicit selfAddress: Address): Address = {
    val host = address.host.getOrElse(selfAddress.host.getOrElse("empty")) // host and port are only undefined if actor.path.address is called for a local actor -> use selfAddress
    val port = address.port.getOrElse(selfAddress.port.getOrElse(-1))
    if(port == publisherBasePort - 1) // simulator
      Address("tcp", clusterName, "simulator", publisherBasePort - 1)
    else if(publisherBasePort until publisherBasePort + nSpeedPublishers contains port)
      Address("tcp", clusterName, s"speedPublisher${port % publisherBasePort + 1}", port)
    else if(port == publisherBasePort + nSpeedPublishers)
      Address("tcp", clusterName, s"densityPublisher", port)
    else Address("tcp", clusterName, host, port)
  }

  private def shrinkOperatorName(str: String): String = {
    val split = str.split("Node")
    split(0) + "-" + split(1).substring(0, 6) // "ConjunctionNodedf02..." -> "Conjunction-df02..."
  }
  /**
    * generate JSON Objects describing the parents of an operator
    * addition for mininet simulation: convert the parent names (not the path with address, only the name) of stream and sequence operators from IPs to names
    * times are the migrating child operator's times, not the ones of the parent operators
   */
  private def convertParentList(parents: Seq[ActorRef], timestamp: Long, newHostInfo: HostInfo)(implicit selfAddress: Address): List[JSONObject] = {

    val parentList: List[JSONObject] = parents.toList.map(parent => {
      val pName = if(parent.path.name.contains("P:")) {
        convertAddress(parent.path.address).host.getOrElse("defaultParent") // "P:10.0.0.101:2520" -> "speedPublisher1"
      } else shrinkOperatorName(parent.path.name)
      JSONObject(Map(
        "operatorName" -> pName,
        "bandwidthDelayProduct" -> newHostInfo.operatorMetrics.operatorToParentBDP.getOrElse(parent.toString(), -1.0),
        "messageOverhead" -> newHostInfo.operatorMetrics.accPlacementMsgOverhead,
        "timestamp" -> timestamp
      ))
    })
    parentList
  }

  /**
    * Sends the GUI server information of an operator transition
    *
    * @param oldOperator - Previous address of the operator
    * @param newOperator - New address of the operator
    * @param algorithmName - Placement algorithm used when placement got updated
    * @param migrationTime - Migration time of the transition
    * @param parents - Parent actor names (without path)
    */
  def sendOperatorTransitionUpdate(oldOperator: ActorRef, newOperator: ActorRef, algorithmName: String, timestamp: Long, migrationTime: Long, nodeSelectionTime: Long, parents: Seq[ActorRef], newHostInfo: HostInfo, isRootOperator: Boolean)(implicit selfAddress: Address) = Future {
    log.info(s"GUI update data $algorithmName $oldOperator $newOperator")
    val oAddress = if(isMininetSimulation) convertAddress(oldOperator.path.address) else oldOperator.path.address
    val tAddress = if(isMininetSimulation) convertAddress(newOperator.path.address) else newOperator.path.address
    val parentList = convertParentList(parents, timestamp, newHostInfo)
    val data = Map(
      "oldMember" -> JSONObject(Map("host" -> oAddress.host.getOrElse(selfAddress.host.get))),
      "member" -> JSONObject(Map("host" -> tAddress.host.getOrElse(selfAddress.host.get))),
      "oldOperator" -> JSONObject(Map("name" -> shrinkOperatorName(oldOperator.path.name))),
      "migrationTime" -> migrationTime,
      "operator" -> JSONObject(Map(
        "name" -> shrinkOperatorName(newOperator.path.name),
        "algorithm" -> algorithmName,
        "parents" -> JSONArray(parentList),
        "isRootOperator" -> isRootOperator
      ))
    )

    val json = JSONObject(data).toString()
    log.info(s"Sending GUI operator update $json to $guiEndpoint/setOperator")
    try {
      val result = Http(s"$guiEndpoint/setOperator")
        .postData(json)
        .header("content-type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(10000))
        .asString.code
      log.info(s"GUI update result code: $result")
    } catch {
      case e: Throwable => log.debug(s"error while sending gui operator transition update $json", e)
    }
  }

  /**
    * Send initial operator placement to GUI server
    * @param transitionAddress - address of the placed operator
    * @param algorithmName - placement algorithm used for placing the operator
    * @param operatorName - name of the operator
    * @param transitionMode - transition mode that is used for transitions
    * @param parents - parent operators of the current operator
    * @param nodeInfo - node info of the node that the operator was placed on
    */
  def sendInitialOperator(transitionAddress: Address, algorithmName: String, operatorName: String, transitionMode: String, parents: Seq[ActorRef], nodeInfo: HostInfo, isRootOperator: Boolean)(implicit selfAddress: Address): Future[Unit] = Future {
    try {
    val parentList = convertParentList(parents, System.currentTimeMillis(), nodeInfo)
    val tAddress = if(isMininetSimulation) convertAddress(transitionAddress) else transitionAddress
    val data = Map(
      "member" -> JSONObject(Map("host" -> tAddress.host.getOrElse(selfAddress.host.get))),
      "operator" -> JSONObject(Map(
        "name" -> shrinkOperatorName(operatorName),
        "algorithm" -> algorithmName,
        "parents" -> JSONArray(parentList),
        "isRootOperator" -> isRootOperator
      )),
      "transitionMode" -> transitionMode
    )

    val json = JSONObject(data).toString()
    log.info(s"Sending GUI operator update $json to $guiEndpoint/setOperator")


    val result = Http(s"$guiEndpoint/setOperator")
      .postData(json)
      .header("content-type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(10000))
      .asString.code
    log.info(s"GUI update result code: $result")
    } catch {
      case e: Throwable => log.debug(s"error while sending gui operator update $transitionAddress $operatorName $parents $nodeInfo", e)
    }
  }

  /**
    * Send the nodes that are in the cluster to the GUI server
    * @param cluster - cluster that contains the nodes to be send
    */
  def sendMembers(cluster: Cluster): Future[Unit] = Future {
    implicit val selfAddress: Address = cluster.selfAddress
    log.info("Sending cluster member update to GUI")
    val members = cluster.state.members.filter(x => x.status == MemberStatus.Up)
    val hostList = mutable.MutableList[JSONObject]()
    for (member <- members) {
      val address = if(isMininetSimulation) convertAddress(member.address) else member.address
      hostList += scala.util.parsing.json.JSONObject(Map("host" -> address.host.get, "port" -> address.port.getOrElse(-1),"role" -> getPrimaryRole(member)))
    }
    //hostList += JSONObject(Map("host" -> "node0 (Consumer)"))
    val data = scala.util.parsing.json.JSONObject(Map("members" -> scala.util.parsing.json.JSONArray(hostList.toList)))
    log.info(s"Sending GUI update ${data.toString()} to $guiEndpoint/setMembers")
    try {
      val result = Http(s"$guiEndpoint/setMembers")
        .postData(data.toString())
        .header("content-type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(10000))
        .asString.code
      log.info(s"GUI update result code $result")
    } catch {
      case e: Throwable => log.debug(s"error while sending gui member update: \n $e")
    }
  }

  /**
    * Sends the overall Bandwidth Delay Product and vivaldi coordinates to the GUI server
    * @param bdp - the accumulated bandwidth delay product of the operator graph
    * @param latencyDistances - List of distances and vivaldi coordinates
    */
  def sendBDPUpdate(bdp: Double, latencyDistances: List[LatencyDistance])(implicit selfAddress: Address): Future[Unit] = Future {
    log.info(s"GUI update consumer data BDP: $bdp")

    var coordinatesList = mutable.Map[String, JSONObject]()
    latencyDistances.foreach(obj => {
      coordinatesList += (
        convertAddress(obj.member1).host.get ->
        JSONObject(Map("x" -> obj.coord1.x, "y" -> obj.coord1.y, "h" -> obj.coord1.h)))
    })

    var latencyValues = ListBuffer[JSONObject]()
    latencyDistances.foreach(obj => {
      if (obj.member1.host.get != obj.member2.host.get) {
        latencyValues += JSONObject(Map(
          "source" -> convertAddress(obj.member1).host.get,
          "destination" -> convertAddress(obj.member2).host.get,
          "distance" -> obj.distance
        ))
      }
    })

    val data = Map(
      "bandwidthDelayProduct" -> bdp,
      "coordinates" -> JSONObject(coordinatesList.toMap),
      "latencyValues" -> JSONArray(latencyValues.toList)
    )

    val json = JSONObject(data).toString()
    log.info(s"Sending GUI BDP and latency update to $guiEndpoint/consumer")

    try {
      val result = Http(s"$guiEndpoint/consumer")
        .postData(json)
        .header("content-type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(10000))
        .asString.code
      log.info(s"GUI update result code: $result")
    } catch {
      case e: Throwable => log.debug(s"error while sending gui bdp update: \n $e")
    }
  }

  /**
    * Sends the overall transition time to the GUI server
    * @param transitionTime - the accumulated transition time of the operator graph
    */
  def sendTransitionTimeUpdate(transitionTime: Double): Future[Unit] = Future {
    log.info(s"GUI update data transition time $transitionTime")

    val data = Map(
      "time" -> transitionTime
    )

    val json = JSONObject(data).toString()
    log.info(s"Sending GUI operator update $json to $guiEndpoint/setTransitionTime")

    try {
      val result = Http(s"$guiEndpoint/setTransitionTime")
        .postData(json)
        .header("content-type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(10000))
        .asString.code
      log.info(s"GUI update result code: $result")
    } catch {
      case e: Throwable => log.debug(s"error while sending gui transition time update: \n $e")
    }
  }
}
