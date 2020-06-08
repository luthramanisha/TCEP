package tcep.factories

import java.util.UUID

import akka.actor.{ActorContext, ActorRef, Deploy, Props}
import akka.cluster.Cluster
import akka.remote.RemoteScope
import org.slf4j.LoggerFactory
import tcep.data.Queries._
import tcep.graph.nodes._
import tcep.graph.nodes.traits.{TransitionConfig, TransitionModeNames}
import tcep.graph.nodes.traits.TransitionModeNames.Mode
import tcep.graph.{CreatedCallback, EventCallback}
import tcep.machinenodes.helper.actors.{CreateRemoteOperator, RemoteOperatorCreated}
import tcep.placement.HostInfo
import tcep.utils.TCEPUtils

import scala.concurrent.{ExecutionContext, Future}

object NodeFactory {
  /*
  def createConjunctionNode(transitionConfig: TransitionConfig,
                            hostInfo: HostInfo,
                            backupMode: Boolean,
                            mainNode: Option[ActorRef],
                            query: ConjunctionQuery,
                            parentNode1: ActorRef,
                            parentNode2: ActorRef,
                            createdCallback: Option[CreatedCallback],
                            eventCallback: Option[EventCallback],
                            context: ActorContext,
                            isRootOperator: Boolean): ActorRef = {
    context.system.actorOf(Props(
        classOf[ConjunctionNode],
        mode,
        hostInfo,
        backupMode,
        mainNode,
        query,
        parentNode1,
        parentNode2,
        createdCallback,
        eventCallback,
      isRootOperator
      ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"ConjunctionNode${UUID.randomUUID.toString}"
    )
  }

  def createDisjunctionNode(transitionConfig: TransitionConfig, hostInfo: HostInfo,
                            backupMode: Boolean,
                            mainNode: Option[ActorRef],
                            query: DisjunctionQuery,
                            parentNode1: ActorRef,
                            parentNode2: ActorRef,
                            createdCallback: Option[CreatedCallback],
                            eventCallback: Option[EventCallback],
                            context: ActorContext,
                            isRootOperator: Boolean): ActorRef = {

    context.system.actorOf(Props(
        classOf[DisjunctionNode],
        mode,
        hostInfo,
        backupMode,
        mainNode,
        query,
        parentNode1,
        parentNode2,
        createdCallback,
        eventCallback,
      isRootOperator
      ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"DisjunctionNode${UUID.randomUUID.toString}"
    )
  }

  def createDropElementNode(transitionConfig: TransitionConfig,
                            hostInfo: HostInfo,
                            backupMode: Boolean,
                            mainNode: Option[ActorRef],
                            query: DropElemQuery,
                            parentNode: ActorRef,
                            createdCallback: Option[CreatedCallback],
                            eventCallback: Option[EventCallback],
                            context: ActorContext,
                            isRootOperator: Boolean): ActorRef = {
    context.system.actorOf(Props(
        classOf[DropElemNode],
        mode,
        hostInfo,
        backupMode,
        mainNode,
        query,
        parentNode,
        createdCallback,
        eventCallback,
      isRootOperator
      ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"DropElemNode${UUID.randomUUID.toString}"
    )
  }

  def createFilterNode(transitionConfig: TransitionConfig,
                       hostInfo: HostInfo,
                       backupMode: Boolean,
                       mainNode: Option[ActorRef],
                       query: FilterQuery,
                       parentNode: ActorRef,
                       createdCallback: Option[CreatedCallback],
                       eventCallback: Option[EventCallback],
                       context: ActorContext,
                       isRootOperator: Boolean): ActorRef = {
    context.system.actorOf(Props(
        classOf[FilterNode],
        mode,
        hostInfo,
        backupMode,
        mainNode,
        query,
        parentNode,
        createdCallback,
        eventCallback,
      isRootOperator
      ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"FilterNode${UUID.randomUUID.toString}"
    )
  }

  def createJoinNode(transitionConfig: TransitionConfig,
                     hostInfo: HostInfo,
                     backupMode: Boolean,
                     mainNode: Option[ActorRef],
                     query: JoinQuery,
                     parentNode1: ActorRef,
                     parentNode2: ActorRef,
                     createdCallback: Option[CreatedCallback],
                     eventCallback: Option[EventCallback],
                     context: ActorContext,
                     isRootOperator: Boolean): ActorRef = {

    context.system.actorOf(Props(
        classOf[JoinNode],
        transitionConfig,
        hostInfo,
        backupMode,
        mainNode,
        query,
        parentNode1,
        parentNode2,
        createdCallback,
        eventCallback,
      isRootOperator
      ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"JoinNode${UUID.randomUUID.toString}"
    )
  }

  def createSelfJoinNode(transitionConfig: TransitionConfig,
                         hostInfo: HostInfo,
                         backupMode: Boolean,
                         mainNode: Option[ActorRef],
                         query: SelfJoinQuery,
                         parentNode: ActorRef,
                         createdCallback: Option[CreatedCallback],
                         eventCallback: Option[EventCallback],
                         context: ActorContext,
                         isRootOperator: Boolean): ActorRef = {

    context.system.actorOf(Props(
        classOf[SelfJoinNode],
        transitionConfig,
        hostInfo,
        backupMode,
        mainNode,
        query,
        parentNode,
        createdCallback,
        eventCallback,
      isRootOperator
      ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"SelfJoinNode${UUID.randomUUID.toString}"
    )
  }

  def createSequenceNode(transitionConfig: TransitionConfig,
                         hostInfo: HostInfo,
                         backupMode: Boolean,
                         mainNode: Option[ActorRef],
                         query: SequenceQuery,
                         publishers: Seq[ActorRef],
                         createdCallback: Option[CreatedCallback],
                         eventCallback: Option[EventCallback],
                         context: ActorContext,
                         isRootOperator: Boolean): ActorRef = {

    context.system.actorOf(Props(
        classOf[SequenceNode],
        transitionConfig,
        hostInfo,
        backupMode,
        mainNode,
        query,
        publishers,
        createdCallback,
        eventCallback,
      isRootOperator
      ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"),s"SequenceNode${UUID.randomUUID.toString}"
    )
  }

  def createStreamNode( transitionConfig: TransitionConfig,
                        hostInfo: HostInfo,
                        backupMode: Boolean,
                        mainNode: Option[ActorRef],
                        query: StreamQuery,
                        publisher: ActorRef,
                        createdCallback: Option[CreatedCallback],
                        eventCallback: Option[EventCallback],
                        context: ActorContext,
                        isRootOperator: Boolean): ActorRef = {

    context.system.actorOf(Props(
        classOf[StreamNode],
        mode,
        hostInfo,
        backupMode,
        mainNode,
        query,
        publisher,
        createdCallback,
        eventCallback,
      isRootOperator
      ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"StreamNode${UUID.randomUUID.toString}"
    )
  }

  def createAverageNode(transitionConfig: TransitionConfig,
                        hostInfo: HostInfo,
                        backupMode: Boolean,
                        mainNode: Option[ActorRef],
                        query: AverageQuery,
                        parentNode: ActorRef,
                        createdCallback: Option[CreatedCallback],
                        eventCallback: Option[EventCallback],
                        context: ActorContext,
                        isRootOperator: Boolean): ActorRef = {
    context.system.actorOf(Props(
      classOf[AverageNode],
      mode,
      hostInfo,
      backupMode,
      mainNode,
      query,
      parentNode,
      createdCallback,
      eventCallback,
      isRootOperator
    ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"AverageNode${UUID.randomUUID.toString}"
    )
  }

  def createObserveChangeNode(transitionConfig: TransitionConfig,
                              hostInfo: HostInfo,
                              backupMode: Boolean,
                              mainNode: Option[ActorRef],
                              query: ObserveChangeQuery,
                              parentNode: ActorRef,
                              createdCallback: Option[CreatedCallback],
                              eventCallback: Option[EventCallback],
                              context: ActorContext,
                              isRootOperator: Boolean): ActorRef = {
    context.system.actorOf(Props(
      classOf[ObserveChangeNode],
      mode,
      hostInfo,
      backupMode,
      mainNode,
      query,
      parentNode,
      createdCallback,
      eventCallback,
      isRootOperator
    ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"ObserveChangeNode${UUID.randomUUID.toString}"
    )
  }

  def createSlidingWindowNode(transitionConfig: TransitionConfig,
                              hostInfo: HostInfo,
                              backupMode: Boolean,
                              mainNode: Option[ActorRef],
                              query: SlidingWindowQuery,
                              parentNode: ActorRef,
                              createdCallback: Option[CreatedCallback],
                              eventCallback: Option[EventCallback],
                              context: ActorContext,
                              isRootOperator: Boolean): ActorRef = {
    context.system.actorOf(Props(
      classOf[SlidingWindowNode],
      mode,
      hostInfo,
      backupMode,
      mainNode,
      query,
      parentNode,
      createdCallback,
      eventCallback,
      isRootOperator
    ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"SlidingWindowNode${UUID.randomUUID.toString}"
    )
  }

  def createNewAverageNode(transitionConfig: TransitionConfig,
                           hostInfo: HostInfo,
                           backupMode: Boolean,
                           mainNode: Option[ActorRef],
                           query: NewAverageQuery,
                           parentNode: ActorRef,
                           createdCallback: Option[CreatedCallback],
                           eventCallback: Option[EventCallback],
                           context: ActorContext,
                           isRootOperator: Boolean): ActorRef = {
    context.system.actorOf(Props(
      classOf[NewAverageNode],
      mode,
      hostInfo,
      backupMode,
      mainNode,
      query,
      parentNode,
      createdCallback,
      eventCallback,
      isRootOperator
    ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"NewAverageNode${UUID.randomUUID.toString}"
    )
  }

  def createDatabaseJoinNode(transitionConfig: TransitionConfig,
                             hostInfo: HostInfo,
                             backupMode: Boolean,
                             mainNode: Option[ActorRef],
                             query: DatabaseJoinQuery,
                             parentNode: ActorRef,
                             createdCallback: Option[CreatedCallback],
                             eventCallback: Option[EventCallback],
                             context: ActorContext,
                             isRootOperator: Boolean): ActorRef = {
    context.system.actorOf(Props(
      classOf[DatabaseJoinNode],
      mode,
      hostInfo,
      backupMode,
      mainNode,
      query,
      parentNode,
      createdCallback,
      eventCallback,
      isRootOperator
    ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"DatabaseJoinNode${UUID.randomUUID.toString}"
    )
  }

  def createConverterNode(transitionConfig: TransitionConfig,
                          hostInfo: HostInfo,
                          backupMode: Boolean,
                          mainNode: Option[ActorRef],
                          query: ConverterQuery,
                          parentNode: ActorRef,
                          createdCallback: Option[CreatedCallback],
                          eventCallback: Option[EventCallback],
                          context: ActorContext,
                          isRootOperator: Boolean): ActorRef = {
    context.system.actorOf(Props(
      classOf[ConverterNode],
      mode,
      hostInfo,
      backupMode,
      mainNode,
      query,
      parentNode,
      createdCallback,
      eventCallback,
      isRootOperator
    ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"ConverterNode${UUID.randomUUID.toString}"
    )
  }

  def createShrinkingFilterNode(transitionConfig: TransitionConfig,
                                hostInfo: HostInfo,
                                backupMode: Boolean,
                                mainNode: Option[ActorRef],
                                query: ShrinkingFilterQuery,
                                parentNode: ActorRef,
                                createdCallback: Option[CreatedCallback],
                                eventCallback: Option[EventCallback],
                                context: ActorContext,
                                isRootOperator: Boolean): ActorRef = {
    context.system.actorOf(Props(
      classOf[ShrinkingFilterNode],
      mode,
      hostInfo,
      backupMode,
      mainNode,
      query,
      parentNode,
      createdCallback,
      eventCallback,
      isRootOperator
    ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"ShrinkingFilterNode${UUID.randomUUID.toString}"
    )
  }

  def createWindowStatisticNode(transitionConfig: TransitionConfig,
                                hostInfo: HostInfo,
                                backupMode: Boolean,
                                mainNode: Option[ActorRef],
                                query: WindowStatisticQuery,
                                parentNode: ActorRef,
                                createdCallback: Option[CreatedCallback],
                                eventCallback: Option[EventCallback],
                                context: ActorContext,
                                isRootOperator: Boolean): ActorRef = {
    context.system.actorOf(Props(
      classOf[WindowStatisticNode],
      mode,
      hostInfo,
      backupMode,
      mainNode,
      query,
      parentNode,
      createdCallback,
      eventCallback,
      isRootOperator
    ).withDeploy(Deploy(scope = RemoteScope(hostInfo.member.address))).withMailbox("prio-mailbox"), s"WindowStatisticNode${UUID.randomUUID.toString}"
    )
  }
  */

  val log = LoggerFactory.getLogger(getClass)

  def getOperatorTypeFromQuery(query: Query): Class[_] = {
    query match {
      case aq: AverageQuery =>              classOf[AverageNode]
      case sq: StreamQuery =>               classOf[StreamNode]
      case sq: SequenceQuery =>             classOf[SequenceNode]
      case sq: SelfJoinQuery =>             classOf[SelfJoinNode]
      case jq: JoinQuery =>                 classOf[JoinNode]
      case dq: DisjunctionQuery =>          classOf[DisjunctionNode]
      case cq: ConjunctionQuery =>          classOf[ConjunctionNode]
      case dq: DropElemQuery =>             classOf[DropElemNode]
      case fq: FilterQuery =>               classOf[FilterNode]
      case oc: ObserveChangeQuery =>        classOf[ObserveChangeNode]
      case sw: SlidingWindowQuery =>        classOf[SlidingWindowNode]
      case navg: NewAverageQuery =>         classOf[NewAverageNode]
      case db: DatabaseJoinQuery =>         classOf[DatabaseJoinNode]
      case cn: ConverterQuery =>            classOf[ConverterNode]
      case sf: ShrinkingFilterQuery =>      classOf[ShrinkingFilterNode]
      case winStat: WindowStatisticQuery => classOf[WindowStatisticNode]
      case _ => throw new IllegalArgumentException(s"cannot create unknown operator type: $query")
    }
  }

  def createOperator(cluster: Cluster,
                     context: ActorContext,
                     targetHost: HostInfo,
                     props: Props
                    )(implicit ec: ExecutionContext): Future[ActorRef] = {
    if (cluster.selfMember == targetHost.member)
      Future {
        try {
          val name = s"${targetHost.operator.toString.split("\\(", 2).head}${UUID.randomUUID().toString}"
          val ref = cluster.system.actorOf(props.withMailbox("prio-mailbox"), name)
          log.info(s"created operator on self: $ref")
          ref
        } catch {
          case e: Throwable =>
            log.error(s"failed to create operator $targetHost", e)
            log.error(s"props: $props")
            context.self
        }
      }
    else for {
      hostTaskManager <- TCEPUtils.getTaskManagerOfMember(cluster, targetHost.member)
      request <- TCEPUtils.guaranteedDelivery(context, hostTaskManager, CreateRemoteOperator(targetHost, props)).mapTo[RemoteOperatorCreated]
    } yield {
      log.info(s"created operator on other cluster node: ${request.ref}")
      request.ref
    }
  }
}
