package tcep.placement

import akka.actor.{ActorContext, ActorRef}
import akka.cluster.{Cluster, Member}
import org.discovery.vivaldi.Coordinates
import tcep.data.Queries.Query
import tcep.graph.nodes.traits.Node.Dependencies

import scala.concurrent.Future

object RandomAlgorithm extends PlacementStrategy {

  override val name = "Random"

  /**
    * Randomly places the operator on one of the available nodes
    * @param context      actor context
    * @param cluster      cluster of nodes
    * @param dependencies hosts of operators that the operator to be deployed depends on
    * @param askerInfo    HostInfo item containing the address of the node asking to deploy the operator (currently always the clientNode)
    * @return HostInfo, containing the address of the node to host the operator
    */
  def findOptimalNode(context: ActorContext, cluster: Cluster, dependencies: Dependencies, askerInfo: HostInfo, operator: Query): Future[HostInfo] = {
    val candidates: Set[Member] = findPossibleNodesToDeploy(cluster)
    log.info(s"the number of available members are: ${candidates.size}, members: ${candidates.toList.map(c => c.address.host.get)}")
    applyRandomAlgorithm(cluster, operator, candidates.toVector, dependencies.parents)
  }

  def findOptimalNodes(context: ActorContext, cluster: Cluster, dependencies: Dependencies, askerInfo: HostInfo, operator: Query): Future[(HostInfo, HostInfo)] = {
    val candidates: Set[Member] = findPossibleNodesToDeploy(cluster)
    for {
      m1 <- applyRandomAlgorithm(cluster, operator, candidates.toVector, dependencies.parents)
      m2 <- applyRandomAlgorithm(cluster, operator, candidates.filter(!_.equals(m1)).toVector, dependencies.parents)
    } yield (m1, m2)
  }

  /**
    * Applies Random placement algorithm to find a random node for operator deployment
    *
    * @param candidateNodes coordinates of the candidate nodes
    * @return the address of member where operator will be deployed
    */
  def applyRandomAlgorithm(cluster: Cluster, operator: Query, candidateNodes: Vector[Member], parents: List[ActorRef]): Future[HostInfo] = {

    val random: Int = scala.util.Random.nextInt(candidateNodes.size)
    val randomMember: Member = candidateNodes(random)
    for { bdpUpdate <- this.updateOperatorToParentBDP(cluster, operator, randomMember, parents) } yield {
      log.info(s"findOptimalNode - deploying operator on randomly chosen host: ${randomMember}")
      HostInfo(randomMember, operator, this.getPlacementMetrics(operator))
    }
  }

  override def hasInitialPlacementRoutine(): Boolean = false

  override def hasPeriodicUpdate(): Boolean = false

  override def calculateVCSingleOperator(cluster: Cluster, operator: Query, startCoordinates: Coordinates, dependencyCoordinates: List[Coordinates], nnCandidates: Map[Member, Coordinates]): Future[Coordinates] = ???

  override def selectHostFromCandidates(coordinates: Coordinates, memberToCoordinates: Map[Member, Coordinates], operator: Option[Query]): Future[Member] = ???
}
