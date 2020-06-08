package tcep.utils

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{ActorContext, ActorRef, ActorSelection, Address, Props, RootActorPath}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.pattern.{Patterns, ask}
import akka.stream.ConnectionException
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.discovery.vivaldi.{Coordinates, DistVivaldiActor}
import org.slf4j.LoggerFactory
import tcep.machinenodes.helper.actors._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


/**
  * Created by raheel on 18/01/2018.
  * Contains generic utility methods
  */
object TCEPUtils {

  val log = LoggerFactory.getLogger(getClass)
  implicit val timeout = Timeout(ConfigFactory.load().getInt("constants.default-request-timeout"), TimeUnit.SECONDS) // timeout when using single request without retry or total time with retries
  val retryTimeout = Timeout(ConfigFactory.load().getLong("constants.retry-timeout"), TimeUnit.SECONDS) // timeout per try when using retry functionality
  val retries = ConfigFactory.load().getInt("constants.default-retries")
  val isMininetSim: Boolean = ConfigFactory.load().getBoolean("constants.mininet-simulation")

  def executeAndLogTime[R](block: => R, tag: String): R = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    SpecialStats.log(this.getClass.getSimpleName, tag, s"${t1 - t0}")
    result
  }

  /**
    * get the coordinates cached by the local DistVivaldiActor
    * @param cluster
    * @param node
    * @return
    */
  def getCoordinatesOfNode(cluster: Cluster, node: Address)(implicit ec: ExecutionContext): Future[Coordinates] = {

    if(cluster.selfAddress.equals(node)) {
      Future { DistVivaldiActor.localPos.coordinates }
    } else {

      val ask: Future[Coordinates] = for {
        localVivaldiActor <- selectDistVivaldiOn(cluster, cluster.selfAddress).resolveOne()
        coordResponse <- Patterns.ask(localVivaldiActor, CoordinatesRequest(node), timeout).mapTo[CoordinatesResponse]
      } yield coordResponse.coordinate
      ask.onComplete {
        case Success(response) =>
        case Failure(exception) =>
          log.error(s"error while retrieving coordinates of $node from local vivaldiActor: \n ${exception.toString}")
          throw exception
      }
      ask
    }
  }

  def getLoadOfMember(cluster: Cluster, node: Member)(implicit ec: ExecutionContext): Future[Double] = {
    for {
      taskManagerActor <- this.getTaskManagerOfMember(cluster, node)
      request <- trySendWithRetry(taskManagerActor, LoadRequest(), retryTimeout, retries).mapTo[Double]
    } yield request
  }

  def getBandwidthBetweenNodes(cluster: Cluster, source: Member, target: Member)(implicit ec: ExecutionContext): Future[Double] = {
    SpecialStats.debug(this.toString, s"bw request for ${source.address}->${target.address}")
    if(source.equals(target)) Future { 0.0d }
    else {
      for {
        sourceTaskManagerActor <- this.getTaskManagerOfMember(cluster, source)
        request <- trySendWithRetry(sourceTaskManagerActor, SingleBandwidthRequest(target), retryTimeout, retries).mapTo[SingleBandwidthResponse]
      } yield request.bandwidth
    }
  }

  /**
    * retrieves all bandwidth measurements between all nodes (only for mininet where bandwidth is set constant)
    */
  def getAllBandwidthsFromLocalTaskManager(cluster: Cluster)(implicit ec: ExecutionContext): Future[Map[(Member, Member), Double]] = {
    SpecialStats.log(this.toString, "placement", s"asking local taskManager actor for all its bandwidth measurements")
    val request = for {
      taskManager <- this.getTaskManagerOfMember(cluster, cluster.selfMember)
      allBandwidthsResponse <- trySendWithRetry(taskManager, AllBandwidthsRequest(), retryTimeout, retries).mapTo[AllBandwidthsResponse]
    } yield allBandwidthsResponse.bandwidthMap
    request.onComplete {
      case Failure(exception) => SpecialStats.log(this.toString, "placement", s"failed to get all bandwidths from local taskmanager, cause: $exception")
      case Success(value) => SpecialStats.log(this.toString, "placement", s"retrieved all bandwidths from local taskmanager (${value.size} total)")
    }
    request
  }

  def getBDPBetweenNodes(cluster: Cluster, source: Member, target: Member)(implicit ec: ExecutionContext): Future[Double] = {

    if(source.equals(target)) return Future { 0.0d }
    val bdp = for {
      latency <- this.getVivaldiDistance(cluster, source, target)
      bw <- this.getBandwidthBetweenNodes(cluster, source, target).map(bw => bw / 1000.0d) // Mbit/s -> Mbit/ms since vivaldi distance is ~latency in ms
    } yield {
      if(latency <= 0.0d || bw <= 0.0d) log.warn(s"BDP between non-equal nodes $source and $target is zero!")
      latency * bw
    }
    bdp
  }

  /**
    * @return the latency between source and target as approximated by their vivaldi actor coordinate distance
    */
  def getVivaldiDistance(cluster: Cluster, source: Member, target: Member)(implicit ec: ExecutionContext): Future[Double] = {
    val sourceCoordRequest: Future[Coordinates] = getCoordinatesOfNode(cluster, source.address)
    val targetCoordRequest: Future[Coordinates] = getCoordinatesOfNode(cluster, target.address)
    val result = for {
      sourceCoords <- sourceCoordRequest
      targetCoords <- targetCoordRequest
    } yield sourceCoords.distance(targetCoords) // both requests must complete before this future can complete
    result
  }

  def getTaskManagerOfMember(cluster: Cluster, node: Member, remainingRetries: Int = retries)(implicit ec: ExecutionContext): Future[ActorRef] = {
    for {
      localTaskManager <- selectTaskManagerOn(cluster, cluster.selfAddress).resolveOne()(retryTimeout).recoverWith {
        case e: Throwable =>
          if (remainingRetries > 0) {
            SpecialStats.debug(this.getClass.toString, s"failed to resolve local taskManager actor (attempt ${retries - remainingRetries} of $retries) due to ${e.toString}")
            getTaskManagerOfMember(cluster, node, remainingRetries - 1)
          }
          else throw new ConnectionException(s"unable to retrieve taskManager actor of $node after $retries attempts with timeout $retryTimeout, cause: ${e.toString}")
      }
      taskManager <- if(node == cluster.selfAddress) Future { Some(localTaskManager) } else (localTaskManager ? GetTaskManagerActor(node)).mapTo[TaskManagerActorResponse].map(_.maybeRef)
      directTry <- if(taskManager.isEmpty) selectTaskManagerOn(cluster, node.address).resolveOne()(retryTimeout) else Future { taskManager.get } // try to contact it directly as a last resort
    } yield {
      SpecialStats.debug(this.toString, s"retrieved actorRef of taskManager $taskManager")
      directTry
    }
  }

  def selectTaskManagerOn(cluster: Cluster, address: Address): ActorSelection = cluster.system.actorSelection(RootActorPath(address) / "user" / "TaskManager*")
  def selectDistVivaldiOn(cluster: Cluster, address: Address): ActorSelection = cluster.system.actorSelection(RootActorPath(address) / "user" / "DistVivaldiRef*")
  def selectPublisherOn(cluster: Cluster, address: Address): ActorSelection = cluster.system.actorSelection(RootActorPath(address) / "user" / "P:*")
  def selectSimulator(cluster: Cluster): ActorSelection = {
    val simulatorNode = cluster.state.members.find(_.hasRole("Subscriber")).getOrElse(throw new RuntimeException("could not find Subscriber node in cluster!"))
    cluster.system.actorSelection(RootActorPath(simulatorNode.address) / "user" / "SimulationSetup*")
  }
  def getAddrStringFromRef(ref: ActorRef): String = ref.path.address.toString
  def getAddrFromRef(ref: ActorRef): Address = ref.path.address
  def getMemberFromActorRef(cluster: Cluster, ref: ActorRef): Member =
    cluster.state.members.find(m => m.address.toString == ref.path.address.toString).getOrElse({
      log.warn(s"could not find member of actor $ref, returning self ${cluster.selfMember}")
      cluster.selfMember})
  def getPublisherHosts(cluster: Cluster): Map[String, Member] = {
    cluster.state.members.filter(m => m.hasRole("Publisher") && m.status == MemberStatus.up)
      .map(p => s"P:${p.address.host.getOrElse("UnknownHost")}:${p.address.port.getOrElse(0)}" -> p).toMap
  }

  /**
    * finds all publisher actors in the cluster; blocks until all are identified!
    * (only used in PlacementStrategy initialization)
    */
  def getPublisherActors(cluster: Cluster)(implicit ec: ExecutionContext): Future[Map[String, ActorRef]] = {
    // in this implementation, each publisher member (PublisherApp) runs at most one publisher!
    //Await.result(makeMapFuture(publisherHosts.map(m => m._1 -> selectPublisherOn(cluster, m._2.address).resolveOne() )), timeout.duration)
    for {
      taskManager <- this.getTaskManagerOfMember(cluster, cluster.selfMember)
      publisherActors <- (taskManager ? PublisherActorRefsRequest()).mapTo[PublisherActorRefsResponse]
    } yield {
      publisherActors.publisherMap
    }
  }

  /**
    * @param futureValueMap map with future values
    * @return a future that completes once all the value futures have completed
    */
  def makeMapFuture[K,V](futureValueMap: Map[K, Future[V]])(implicit ec: ExecutionContext): Future[Map[K,V]] =
    Future.traverse(futureValueMap) { case (k, fv) => fv.map(k -> _) } map(_.toMap)


  def setOfFutureToFuture[T](seqOfFutures: Set[Future[T]])(implicit ec: ExecutionContext): Future[Set[T]] = {
    def futureToFutureTry[T](f: Future[T]): Future[Try[T]] = f.map(Success(_)).recover { case x => Failure(x) }
    val seqOfFutureTrys = seqOfFutures.map(futureToFutureTry(_))
    val futureSetOfTrys: Future[Set[Try[T]]] = Future.sequence(seqOfFutureTrys)
    val futureSetOfSuccesses: Future[Set[T]] = futureSetOfTrys.map(_.collect { case Success(x) => x })
    futureSetOfSuccesses
  }

  /**
    * Attempts to send a message within the timeout and returns a future to the reply. Receiver must send a reply msg of any kind!
    * Useful when network is changing and messages may be lost due to routing problems or similar
    * Optional: retries sending after timeout if no reply received
    * Pay attention to behaviour of receiver if msg is received, but reply is lost and this function re-sends the msg!
    * @param receiver
    * @param msg
    * @param t timeout duration per attempt
    * @param retriesRemaining optional number of attempts to
    * @param retriesUsed
    * @return Future to the reply, containing either the reply value or the cause of failure
    */
  def trySendWithRetry(receiver: ActorRef, msg: Any, t: Timeout = retryTimeout, retriesRemaining: Int = 0, retriesUsed: Int = 0)(implicit ec: ExecutionContext): Future[Any] = {
    implicit val timeout = t
    val res: Future[Any] = receiver ? msg
    res.recoverWith {
      case e: Throwable =>
      if(retriesRemaining > 0) trySendWithRetry(receiver, msg, t, retriesRemaining - 1, retriesUsed + 1)
      else throw new ConnectionException(s"failed to send msg $msg to $receiver after $retriesUsed with timeout $t, cause: ${e.toString}")
    }
  }

  def trySendUntilReply(receiver: ActorRef, msg: Any, t: Timeout = retryTimeout, retriesUsed: Int = 0,
                        tlf: Option[(String, ActorRef) => Unit] = None, tlp: Option[ActorRef] = None)(implicit ec: ExecutionContext): Future[Any] = {
    implicit val timeout = t
    receiver ? msg recoverWith {
      case e: Throwable =>
        val errorMsg = s"failed to send msg $msg to $receiver after $retriesUsed with timeout $t, cause: ${e.toString}, retrying indefinitely"
        if(tlf.isDefined && tlp.isDefined) tlf.get(errorMsg, tlp.get)
        else SpecialStats.debug(receiver.toString(), errorMsg)
        trySendUntilReply(receiver, msg, t, retriesUsed + 1, tlf, tlp)
    }
  }

  def guaranteedDelivery(context: ActorContext, receiver: ActorRef, msg: Message,
                         singleTimeout: Timeout = retryTimeout, totalTimeout: Timeout = timeout,
                                       tlf: Option[(String, ActorRef) => Unit] = None, tlp: Option[ActorRef] = None)(implicit ec: ExecutionContext): Future[Message] = {
    val deliverer = context.actorOf(Props(new GuaranteedMessageDeliverer[Message](tlf, tlp, singleTimeout)), s"deliverer${UUID.randomUUID.toString}")
    val delivery = for {
      response <- (deliverer ? CriticalMessage(receiver, msg))(totalTimeout).mapTo[Message]
    } yield {
      context.stop(deliverer)
      response
    }
    delivery.onComplete {
      case Success(value) =>
        if(tlf.isDefined && tlp.isDefined) tlf.get(s"successfully delivered $msg to $receiver, reply: $value", tlp.get)
        context.stop(deliverer)
      case Failure(e) =>
        if(tlf.isDefined && tlp.isDefined) tlf.get(s"failed to deliver message $msg to $receiver, cause: ${e.toString} ;;;;${e.getStackTraceString}", tlp.get)
        context.stop(deliverer)
    }
    delivery
  }
}