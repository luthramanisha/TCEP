package tcep.graph

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, ActorRef, Props}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.util.Timeout
import org.discovery.vivaldi.Coordinates
import org.slf4j.LoggerFactory
import tcep.data.Events.Event
import tcep.data.Queries._
import tcep.factories.NodeFactory
import tcep.graph.nodes._
import tcep.graph.nodes.traits.Node.Dependencies
import tcep.graph.nodes.traits.TransitionConfig
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.qos.MonitorFactory
import tcep.graph.transition.MAPEK._
import tcep.graph.transition._
import tcep.machinenodes.consumers.Consumer.SetQosMonitors
import tcep.placement.sbon.PietzuchAlgorithm
import tcep.placement.{HostInfo, PlacementStrategy}
import tcep.simulation.tcep.{AllRecords, GUIConnector}
import tcep.utils.SpecialStats

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
  * Created by raheel
  * on 15/08/2017.
  *
  * Extracts the Operator Graph from Base Query
  */
class QueryGraph(context: ActorContext, cluster: Cluster, query: Query, transitionConfig: TransitionConfig, publishers: Map[String, ActorRef], startingPlacementStrategy: Option[PlacementStrategy], createdCallback: Option[CreatedCallback], monitors: Array[MonitorFactory], allRecords: Option[AllRecords] = None, consumer: ActorRef = null, fixedSimulationProperties: Map[Symbol, Int] = Map(), mapekType: String = "requirementBased") {

  val log = LoggerFactory.getLogger(getClass)
  implicit val timeout = Timeout(15 seconds)
  implicit val ec = context.system.dispatcher
  var mapek: MAPEK = MAPEK.createMAPEK(mapekType, context, query, transitionConfig, publishers, startingPlacementStrategy, allRecords, consumer, fixedSimulationProperties)
  val placementStrategy: PlacementStrategy = PlacementStrategy.getStrategyByName(Await.result(mapek.knowledge ? GetPlacementStrategyName, timeout.duration).asInstanceOf[String])
  var clientNode: ActorRef = _

  def createAndStart(clientMonitors: Array[MonitorFactory])(eventCallback: Option[EventCallback]): ActorRef = {
    log.info(s"Creating and starting new QueryGraph with placement ${if(startingPlacementStrategy.isDefined) startingPlacementStrategy.get.name else "default (depends on MAPEK implementation)"}")
    val root = startDeployment(query, transitionConfig, publishers, createdCallback, eventCallback, monitors)
    consumer ! SetQosMonitors
    Thread.sleep(1500)
    clientNode = context.system.actorOf(Props(classOf[ClientNode], root, mapek, consumer), s"ClientNode-${UUID.randomUUID.toString}")
    mapek.knowledge ! SetClient(clientNode)
    mapek.knowledge ! SetTransitionMode(transitionConfig)
    mapek.knowledge ! SetDeploymentStatus(true)
    Thread.sleep(1000) // wait a bit here to avoid glitch where last addOperator msg arrives at knowledge AFTER StartExecution msg is sent
    mapek.knowledge ! NotifyOperators(StartExecution(startingPlacementStrategy.getOrElse(PietzuchAlgorithm).name))
    log.info(s"started query ${query} \n in mode ${transitionConfig} with PlacementAlgorithm ${placementStrategy.name}")
    root
  }

  def stop(): Unit = {
    mapek.stop()
    clientNode ! ShutDown()
  }

  protected def startDeployment(q: Query, mode: TransitionConfig, publishers: Map[String, ActorRef], createdCallback: Option[CreatedCallback], eventCallback: Option[EventCallback], monitors: Array[MonitorFactory]): ActorRef = {

    val startTime = System.currentTimeMillis()
    val res = for {
      init <- placementStrategy.initialize(cluster, false)
    } yield {
      SpecialStats.debug(s"$this", s"starting initial virtual placement")
      val deploymentComplete: Future[ActorRef] = if (placementStrategy.hasInitialPlacementRoutine()) { // some placement algorithms calculate an initial placement with global knowledge, instead of calculating the optimal node one after another
        val initialOperatorPlacementRequest = placementStrategy.initialVirtualOperatorPlacement(cluster, q)
        initialOperatorPlacementRequest.onComplete {
          case Success(value) => SpecialStats.debug(s"$this", s"initial deployment virtual placement took ${System.currentTimeMillis() - startTime}ms")
          case Failure(exception) => SpecialStats.debug(s"$this", s"initial deployment virtual placement failed after ${System.currentTimeMillis() - startTime}ms, cause: \n $exception")
        }
        for {
          initialOperatorPlacement <- initialOperatorPlacementRequest
          deployment <- deployGraph(q, transitionConfig, publishers, createdCallback, eventCallback, monitors, initialPlacement = initialOperatorPlacement, isRootOperator = true)
        } yield deployment
      } else {
        deployGraph(q, transitionConfig, publishers, createdCallback, eventCallback, monitors, isRootOperator = true)
      }
      deploymentComplete
    }
    val rootOperator = Await.result(res.flatten, new FiniteDuration(120, TimeUnit.SECONDS)) // block here to wait until deployment is finished
    SpecialStats.debug(s"$this", s"initial deployment took ${System.currentTimeMillis() - startTime}ms")
    rootOperator
  }

  protected def deployGraph(q: Query, mode: TransitionConfig, publishers: Map[String, ActorRef], createdCallback: Option[CreatedCallback], eventCallback: Option[EventCallback], monitors: Array[MonitorFactory], initialPlacement: Map[Query, Coordinates] = null, isRootOperator: Boolean = false): Future[ActorRef] = {

    val rootOperator: Future[ActorRef] = q match {

      case query: StreamQuery => {
        deploy(transitionConfig, context, cluster, monitors, query, initialPlacement, createdCallback, eventCallback, isRootOperator, publishers(query.publisherName))
      }

      case query: SequenceQuery => {
        deploy(transitionConfig, context, cluster, monitors, query, initialPlacement, createdCallback, eventCallback, isRootOperator, publishers(query.s1.publisherName), publishers(query.s2.publisherName))
      }

      case query: UnaryQuery => {
        deployGraph(query.sq, transitionConfig, publishers, None, None, monitors, initialPlacement) map { parentDeployment =>
          deploy(transitionConfig, context, cluster, monitors, query, initialPlacement, createdCallback, eventCallback, isRootOperator, parentDeployment)
        } flatten
      }

      case query: BinaryQuery => {
        // declare outside for comprehension so that futures start in parallel
        val parent1Deployment = deployGraph(query.sq1, transitionConfig, publishers, None, None, monitors, initialPlacement)
        val parent2Deployment = deployGraph(query.sq2, transitionConfig, publishers, None, None, monitors, initialPlacement)
        val deployment: Future[Future[ActorRef]] = for {
          parent1 <- parent1Deployment
          parent2 <- parent2Deployment
        } yield deploy(transitionConfig, context, cluster, monitors, query, initialPlacement, createdCallback, eventCallback, isRootOperator, parent1, parent2)
        deployment.flatten
      }
      case other => throw new RuntimeException(s"unknown query type! $other")
    }
    rootOperator
  }

  protected def deploy(mode: TransitionConfig, context: ActorContext, cluster: Cluster, factories: Array[MonitorFactory], operator: Query, initialOperatorPlacement: Map[Query, Coordinates], createdCallback: Option[CreatedCallback], eventCallback: Option[EventCallback], isRootOperator: Boolean, parentOperators: ActorRef*): Future[ActorRef] = {

    val reliabilityReqPresent = (query.requirements collect { case r: ReliabilityRequirement => r }).nonEmpty
    SpecialStats.debug(s"$this", s"deploying operator $operator in mode $mode with placement strategy ${placementStrategy.name}")
    val deployment: Future[(ActorRef, HostInfo)] = {
      if (placementStrategy.hasInitialPlacementRoutine() && initialOperatorPlacement.contains(operator)) {

        for {
          hostInfo <- placementStrategy.findHost(initialOperatorPlacement(operator), null, operator, parentOperators.toList)
          deployedOperator <- deployNode(transitionConfig, context, cluster, operator, createdCallback, eventCallback, hostInfo, false, None, isRootOperator, parentOperators:_*)
        } yield {
          if (reliabilityReqPresent) { // for now, start duplicate on self (just like relaxation does in this case, see findOptimalNodes())
            val backupDeployment = deployNode(transitionConfig, context, cluster, operator, createdCallback, eventCallback, HostInfo(cluster.selfMember, operator, hostInfo.operatorMetrics), true, None, isRootOperator, parentOperators:_*)
            backupDeployment.foreach { backup => mapek.knowledge ! AddBackupOperator(backup) }
          }
          (deployedOperator, hostInfo)
        }

      } else { // no initial placement or operator is missing
        if (reliabilityReqPresent) {
          for {
            hostInfo <- placementStrategy.findOptimalNodes(context, cluster, Dependencies(parentOperators.toList, List()), HostInfo(cluster.selfMember, operator), operator)
            deployedOperator <- deployNode(transitionConfig, context, cluster, operator, createdCallback, eventCallback, hostInfo._1, false, None, isRootOperator, parentOperators:_*)
          } yield {
            // for now, start duplicate on self (just like relaxation does in this case)
            val backupDeployment = deployNode(transitionConfig, context, cluster, operator, createdCallback, eventCallback, HostInfo(cluster.selfMember, operator, hostInfo._2.operatorMetrics), true, None, isRootOperator, parentOperators:_*)
            backupDeployment.foreach { backup => mapek.knowledge ! AddBackupOperator(backup) }
            (deployedOperator, hostInfo._1)
          }

        } else { // no reliability requirement
          for {
            hostInfo <- placementStrategy.findOptimalNode(context, cluster, Dependencies(parentOperators.toList, List()), HostInfo(cluster.selfMember, operator), operator)
            deployedOperator <- deployNode(transitionConfig, context, cluster, operator, createdCallback, eventCallback, hostInfo, false, None, isRootOperator, parentOperators:_*)
          } yield {
            (deployedOperator, hostInfo)
          }
        }
      }
    }
    deployment map { opAndHostInfo => {
      SpecialStats.debug(s"$this", s"deployed ${opAndHostInfo._1} on; ${opAndHostInfo._2.member} " +
        s"; with hostInfo ${opAndHostInfo._2.operatorMetrics} " +
        s"; parents: $parentOperators; path.name: ${parentOperators.head.path.name}")
      mapek.knowledge ! AddOperator(opAndHostInfo._1)
      GUIConnector.sendInitialOperator(opAndHostInfo._2.member.address, placementStrategy.name, opAndHostInfo._1.path.name, s"$mode", parentOperators, opAndHostInfo._2, isRootOperator)(selfAddress = cluster.selfAddress)
      opAndHostInfo._1
    }}
  }

  protected def deployNode(mode: TransitionConfig, context: ActorContext, cluster: Cluster, query: Query, createdCallback: Option[CreatedCallback], eventCallback: Option[EventCallback], hostInfo: HostInfo, backupMode: Boolean, mainNode: Option[ActorRef], isRootOperator: Boolean, parentOperators: ActorRef*): Future[ActorRef] = {
    val operatorType = NodeFactory.getOperatorTypeFromQuery(query)
    val props = Props(operatorType, transitionConfig, hostInfo, backupMode, mainNode, query, createdCallback, eventCallback, isRootOperator, parentOperators)
    NodeFactory.createOperator(cluster, context, hostInfo, props)
  }

  def getPlacementStrategy(): String = {
    Await.result(mapek.knowledge ? GetPlacementStrategyName, timeout.duration).asInstanceOf[String]
  }

  def addDemand(demand: Seq[Requirement]): Unit = {
    log.info("Requirements changed. Notifying Monitor")
    mapek.monitor ! AddRequirement(demand)
  }

  def removeDemand(demand: Seq[Requirement]): Unit = {
    log.info("Requirements changed. Notifying Monitor")
    mapek.monitor ! RemoveRequirement(demand)
  }

  def manualTransition(algorithmName: String): Unit = {
    log.info(s"Manual transition request to algorithm $algorithmName")
    mapek.planner ! ManualTransition(algorithmName)
  }
}

//Closures are not serializable so callbacks would need to be wrapped in a class
abstract class CreatedCallback() {
  def apply(): Any
}

abstract class EventCallback() {
  def apply(event: Event): Any
}