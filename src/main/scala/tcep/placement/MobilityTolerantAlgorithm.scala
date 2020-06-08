package tcep.placement

import akka.actor.{ActorContext, ActorRef}
import akka.cluster.{Cluster, Member}
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.utils.TCEPUtils

import scala.concurrent.Future

object MobilityTolerantAlgorithm extends PlacementStrategy {

  override val name = "ProducerConsumer"

  /**
    * places all stream operators directly on their sources, all other operators on the client node
    * @param context      actor context
    * @param cluster      cluster of nodes
    * @param dependencies hosts of operators that the operator to be deployed depends on
    * @param askerInfo    HostInfo item containing the address of the node asking to deploy the operator (currently always the clientNode)
    * @return HostInfo, containing the address of the node to host the operator
    */
  def findOptimalNode(context: ActorContext, cluster: Cluster, dependencies: Dependencies, askerInfo: HostInfo, operator: Query): Future[HostInfo] = {
    val candidates: Set[Member] = findPossibleNodesToDeploy(cluster)
    log.info(s"the number of available members are: ${candidates.size}, members: ${candidates.toList.map(c => c.address.host.get)}")
    applyMobilityTolerantAlgorithm(context, cluster, dependencies.parents, operator)
  }

  override def findOptimalNodes(context: ActorContext, cluster: Cluster, dependencies: Dependencies, askerInfo: HostInfo, operator: Query): Future[(HostInfo, HostInfo)] = {
    throw new RuntimeException("Random algorithm does not support reliability")
  }

  /**
    * Places an operator on the source if it is a stream operator, otherwise on the client node
    *
    * @param dependencies actorRef of the operator(s) the current operator depends on
    * @return the address of member where operator will be deployed
    */
  def applyMobilityTolerantAlgorithm(context: ActorContext, cluster: Cluster,
                                     parents: List[ActorRef], operator: Query): Future[HostInfo] = {

    if(parents.head.path.name.contains("P:")) {
      val parentMember = TCEPUtils.getMemberFromActorRef(cluster, parents.head)
      for { bdpUpdate <- this.updateOperatorToParentBDP(cluster, operator, parentMember, parents) } yield {
        log.info(s"deploying STREAM operator on its publisher: ${parents.head.path}")
        HostInfo(parentMember, operator, this.getPlacementMetrics(operator))
      }
    } else {
      //akka.tcp://adaptiveCEP@10.0.0.253:2500
      val client = cluster.state.members.find(_.hasRole("Subscriber")).getOrElse(cluster.selfMember)

      for { bdpUpdate <- this.updateOperatorToParentBDP(cluster, operator, client, parents) } yield {
        log.info(s"deploying non-stream operator on clientNode ${client}")
        HostInfo(client, operator, this.getPlacementMetrics(operator))
      }
    }
  }

  override def hasInitialPlacementRoutine(): Boolean = false

  override def hasPeriodicUpdate(): Boolean = false

  override def calculateVCSingleOperator(cluster: Cluster, operator: Query, startCoordinates: Coordinates, dependencyCoordinates: List[Coordinates], nnCandidates: Map[Member, Coordinates]): Future[Coordinates] = ???

  override def selectHostFromCandidates(coordinates: Coordinates, memberToCoordinates: Map[Member, Coordinates], operator: Option[Query]): Future[Member] = ???
}
