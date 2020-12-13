package com.tristanpenman.chordial.core

import java.net.InetSocketAddress

import akka.actor._
import akka.event.EventStream
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Event.{LookupCompleted, LookupStarted}
import com.tristanpenman.chordial.core.Router.{Register, RegisterOk}
import com.tristanpenman.chordial.core.algorithms.CheckPredecessorAlgorithm._
import com.tristanpenman.chordial.core.algorithms.ClosestPrecedingNodeAlgorithm._
import com.tristanpenman.chordial.core.algorithms.FindPredecessorAlgorithm._
import com.tristanpenman.chordial.core.algorithms.FindSuccessorAlgorithm._
import com.tristanpenman.chordial.core.algorithms.FixFingersAlgorithm._
import com.tristanpenman.chordial.core.algorithms.NotifyAlgorithm._
import com.tristanpenman.chordial.core.algorithms.StabilisationAlgorithm._
import com.tristanpenman.chordial.core.algorithms.VoluntaryLeaveAlgorithm.{VoluntaryLeaveOk, VoluntaryLeaveStart}
import com.tristanpenman.chordial.core.algorithms._
import com.tristanpenman.chordial.core.shared.NodeInfo

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

final class Node(nodeId: Long,
                 nodeAddr: InetSocketAddress,
                 config: ChordConfig,
                 eventStream: EventStream,
                 router: ActorRef)
    extends Actor
    with ActorLogging
    with Stash {

  import Node._
  import Pointers._

  private val keyspaceBits = config.keyspaceBits
  private val algorithmTimeout = config.algorithmTimeout
  private val externalRequestTimeout = config.externalRequestTimeout
  private val checkPredecessorDelay = config.checkPredecessorDelay
  private val checkPredecessorTimeout = config.checkPredecessorTimeout
  private val stabilizationDelay = config.stabilizationDelay
  private val stabilizationTimeout = config.stabilizationTimeout
  private val fixFingersDelay = config.fixFingersDelay
  private val fixFingersTimeout = config.fixFingersTimeout

  require(keyspaceBits > 0, "keyspaceBits must be a positive Int value")

  private val nodeInfo = NodeInfo(nodeId, nodeAddr, self)

  private val idModulus = 1 << keyspaceBits

  // Finger table ranges from (nodeId + 2^1) up to (nodeId + 2^(keyspace - 1))
  private def fingerTableSize = keyspaceBits

//  private val checkPredecessorTimeout = Timeout(2500.milliseconds)
//  private val stabiliseTimeout = Timeout(1500.milliseconds)
//  private val fixFingersTimeout: Timeout = externalRequestTimeout.duration * 10

  // Check that node ID is reasonable
  require(nodeId >= 0, "ownId must be a non-negative Long value")
  require(nodeId < idModulus, s"ownId must be less than $idModulus (2^$keyspaceBits})")

  router ! Register(nodeId, self)

  private def newPointers(nodeId: Long, seedInfo: NodeInfo) =
    context.actorOf(Pointers.props(nodeId, fingerTableSize, seedInfo, eventStream))

  private def newCheckPredecessorAlgorithm(nodeRef: ActorRef) =
      context.actorOf(CheckPredecessorAlgorithm.props(router, nodeRef, externalRequestTimeout))

  private def newStabilisationAlgorithm(pointersRef: ActorRef) =
    context.actorOf(StabilisationAlgorithm.props(router, nodeInfo, pointersRef, externalRequestTimeout))

  private def newFixFingersAlgorithm(pointersRef: ActorRef) =
    context.actorOf(FixFingersAlgorithm.props(nodeInfo, pointersRef, keyspaceBits))

  private def closestPrecedingFinger(nodeRef: ActorRef, queryId: Long, replyTo: ActorRef) = {
    val algorithm =
      context.actorOf(ClosestPrecedingNodeAlgorithm.props(nodeInfo, nodeRef, fingerTableSize, externalRequestTimeout))
    algorithm
      .ask(ClosestPrecedingNodeAlgorithmStart(queryId))(algorithmTimeout)
      .mapTo[ClosestPrecedingNodeAlgorithmStartResponse]
      .map {
        case ClosestPrecedingNodeAlgorithmFinished(finger) =>
          ClosestPrecedingNodeOk(finger)
        case ClosestPrecedingNodeAlgorithmAlreadyRunning =>
          throw new Exception("ClosestPrecedingFingerAlgorithm actor already running")
        case ClosestPrecedingNodeAlgorithmError(message) =>
          ClosestPrecedingNodeError(message)
      }
      .recover {
        case exception => ClosestPrecedingNodeError(exception.getMessage)
      }
      .pipeTo(replyTo)
  }

  /**
    * Create an actor to execute the FindPredecessor algorithm and, when it finishes/fails, produce a response that
    * will be recognised by the original sender of the request
    *
    * This method passes in the ID and ActorRef of the current node as the initial candidate node, which means the
    * FindPredecessor algorithm will begin its search at the current node.
    */
  private def findPredecessor(queryId: Long, sender: ActorRef, lookupId: Option[Long] = None): Unit = {
    // The FindPredecessorAlgorithm actor will shutdown immediately after it sends a FindPredecessorAlgorithmOk or
    // FindPredecessorAlgorithmError message.  However, if the future returned by the 'ask' request does not complete
    // within the timeout period, the actor must be shutdown manually to ensure that it does not run indefinitely.
    val findPredecessorAlgorithm = context.actorOf(FindPredecessorAlgorithm.props(router, eventStream, lookupId))
    findPredecessorAlgorithm
      .ask(FindPredecessorAlgorithmStart(queryId, nodeInfo))(algorithmTimeout)
      .mapTo[FindPredecessorAlgorithmStartResponse]
      .map {
        case FindPredecessorAlgorithmOk(predecessor) =>
          FindPredecessorOk(queryId, predecessor)
        case FindPredecessorAlgorithmAlreadyRunning =>
          throw new Exception("FindPredecessorAlgorithm actor already running")
        case FindPredecessorAlgorithmError(message) =>
          FindPredecessorError(queryId, message)
      }
      .recover {
        case exception =>
          context.stop(findPredecessorAlgorithm)
          FindPredecessorError(queryId, exception.getMessage)
      }
      .pipeTo(sender)
  }

  /**
    * Create an actor to execute the FindSuccessor algorithm and, when it finishes/fails, produce a response that
    * will be recognised by the original sender of the request.
    *
    * This method passes in the ActorRef of the current node as the search node, which means the operation will be
    * performed in the context of the current node.
    */
  private def findSuccessor(queryId: Long, sender: ActorRef, lookupId: Option[Long] = None): Unit = {
    if (!lookupId.isEmpty)
      eventStream.publish(LookupStarted(nodeId, queryId, lookupId.get))

    // The FindSuccessorAlgorithm actor will shutdown immediately after it sends a FindSuccessorAlgorithmOk or
    // FindSuccessorAlgorithmError message. However, if the future returned by the 'ask' request does not complete
    // within the timeout period, the actor must be shutdown manually to ensure that it does not run indefinitely.
    val findSuccessorAlgorithm = context.actorOf(FindSuccessorAlgorithm.props(router, lookupId))
    findSuccessorAlgorithm
      .ask(FindSuccessorAlgorithmStart(queryId, self))(algorithmTimeout)
      .mapTo[FindSuccessorAlgorithmStartResponse]
      .map {
        case FindSuccessorAlgorithmOk(successor) =>
          if (!lookupId.isEmpty)
            eventStream.publish(LookupCompleted(nodeId, queryId, successor.id, lookupId.get))
          FindSuccessorOk(queryId, successor)
        case FindSuccessorAlgorithmAlreadyRunning =>
          throw new Exception("FindSuccessorAlgorithm actor already running")
        case FindSuccessorAlgorithmError(message) =>
          FindSuccessorError(queryId, message)
      }
      .recover {
        case exception =>
          context.stop(findSuccessorAlgorithm)
          FindSuccessorError(queryId, exception.getMessage)
      }
      .pipeTo(sender)
  }

  private def notify(nodeRef: ActorRef, candidate: NodeInfo, replyTo: ActorRef) = {
    val notifyAlgorithm = context.actorOf(NotifyAlgorithm.props())
    notifyAlgorithm
      .ask(NotifyAlgorithmStart(nodeInfo, candidate, nodeRef))(algorithmTimeout)
      .mapTo[NotifyAlgorithmStartResponse]
      .map {
        case NotifyAlgorithmOk(predecessorUpdated: Boolean) =>
          if (predecessorUpdated) NotifyOk else NotifyIgnored
        case NotifyAlgorithmAlreadyRunning =>
          throw new Exception("NotifyAlgorithm actor already running")
        case NotifyAlgorithmError(message) =>
          NotifyError(message)
      }
      .recover {
        case exception =>
          context.stop(notifyAlgorithm)
          NotifyError(exception.getMessage)
      }
      .pipeTo(replyTo)
  }

  private def scheduleCheckPredecessor(checkPredecessorAlgorithmRef: ActorRef) =
    context.system.scheduler.schedule(checkPredecessorDelay, checkPredecessorDelay) {
      checkPredecessorAlgorithmRef
        .ask(CheckPredecessorAlgorithmStart)(checkPredecessorTimeout)
        .mapTo[CheckPredecessorAlgorithmStartResponse]
        .map {
          case CheckPredecessorAlgorithmAlreadyRunning =>
            log.warning("CheckPredecessor already in progress")
          case CheckPredecessorAlgorithmFinished =>
//            log.debug("CheckPredecessor finished")
          case CheckPredecessorAlgorithmError(message) =>
            log.error("CheckPredecessor failed: {}", message)
        }
        .recover {
          case ex =>
            log.error("CheckPredecessor recovery failed: {}", ex.getMessage)
        }
    }

  private def scheduleStabilisation(stabilisationAlgorithmRef: ActorRef) =
    context.system.scheduler.schedule(stabilizationDelay, stabilizationDelay) {
      stabilisationAlgorithmRef
        .ask(StabilisationAlgorithmStart)(stabilizationTimeout)
        .mapTo[StabilisationAlgorithmStartResponse]
        .map {
          case StabilisationAlgorithmAlreadyRunning =>
            log.warning("Stabilisation already in progress")
          case StabilisationAlgorithmFinished =>
//             log.info("Stabilisation algorithm finished")
          case StabilisationAlgorithmError(message) =>
            log.error("Stabilisation failed: {}", message)
        }
        .recover {
          case ex =>
            log.error("Stabilisation recovery failed: {}", ex.getMessage)
        }
    }

  private def scheduleFixFingers(fixFingersAlgorithmRef: ActorRef) =
    context.system.scheduler.schedule(fixFingersDelay, fixFingersDelay) {
      fixFingersAlgorithmRef
        .ask(FixFingersAlgorithmStart)(fixFingersTimeout)
        .mapTo[FixFingersAlgorithmStartResponse]
        .map {
          case FixFingersAlgorithmAlreadyRunning =>
            log.warning("FixFingers already in progress")
          case FixFingersAlgorithmOk =>
//           log.info("FixFingers finished")
          case FixFingersAlgorithmError(message) =>
            log.error("FixFingers failed: {}", message)
        }
        .recover {
          case ex =>
            log.error("FixFingers recovery failed: {}", ex.getMessage)
        }
    }

  def receiveWhileReady(pointersRef: ActorRef,
                        checkPredecessorAlgorithm: ActorRef,
                        checkPredecessorCancellable: Cancellable,
                        stabilisationAlgorithm: ActorRef,
                        stabilisationCancellable: Cancellable,
                        fixFingersAlgorithm: ActorRef,
                        fixFingersCancellable: Cancellable): Receive = {
    case ClosestPrecedingNode(queryId: Long) =>
      closestPrecedingFinger(pointersRef, queryId, sender())

    case m @ GetId =>
      pointersRef.ask(m)(externalRequestTimeout).pipeTo(sender())

    case m @ GetPredecessor =>
      pointersRef.ask(m)(externalRequestTimeout).pipeTo(sender())

    case m @ GetSuccessor =>
      pointersRef.ask(m)(externalRequestTimeout).pipeTo(sender())

    case m @ UpdatePredecessor(predecessor) =>
      pointersRef.ask(m)(algorithmTimeout).pipeTo(sender())

    case m @ UpdateSuccessor(successor) =>
      pointersRef.ask(m)(algorithmTimeout).pipeTo(sender())

    case FindPredecessor(queryId, lookupId) =>
      findPredecessor(queryId, sender(), lookupId)

    case FindSuccessor(queryId, lookupId) =>
      findSuccessor(queryId, sender(), lookupId)

    case Join(seedId, seedAddr, seedRef) =>
      checkPredecessorCancellable.cancel()
      stabilisationCancellable.cancel()
      fixFingersCancellable.cancel()

      context.stop(pointersRef)
      context.stop(checkPredecessorAlgorithm)
      context.stop(stabilisationAlgorithm)
      context.stop(fixFingersAlgorithm)

      val seedInfo = NodeInfo(seedId, seedAddr, seedRef)
      val newPointersRef = newPointers(nodeId, seedInfo)

      val newCheckPredecessorAlgorithmRef = newCheckPredecessorAlgorithm(newPointersRef)
      val newCheckPredecessorCancellable = scheduleCheckPredecessor(newCheckPredecessorAlgorithmRef)

      val newStabilisationAlgorithmRef = newStabilisationAlgorithm(newPointersRef)
      val newStabilisationCancellable = scheduleStabilisation(newStabilisationAlgorithmRef)

      val newFixFingersAlgorithmRef = newFixFingersAlgorithm(newPointersRef)
      val newFixFingersCancellable = scheduleFixFingers(newFixFingersAlgorithmRef)

      context.become(
        receiveWhileReady(
          newPointersRef,
          newCheckPredecessorAlgorithmRef,
          newCheckPredecessorCancellable,
          newStabilisationAlgorithmRef,
          newStabilisationCancellable,
          newFixFingersAlgorithmRef,
          newFixFingersCancellable
        )
      )

      sender() ! JoinOk


    case Notify(candidateId, candidateAddr, candidateRef) =>
      notify(pointersRef, NodeInfo(candidateId, candidateAddr, candidateRef), sender())

    case Terminate =>
      // tear down scheduled processes and stop children
      checkPredecessorCancellable.cancel()
      stabilisationCancellable.cancel()
      fixFingersCancellable.cancel()
      context.stop(checkPredecessorAlgorithm)
      context.stop(stabilisationAlgorithm)
      context.stop(fixFingersAlgorithm)

      val voluntaryLeaveAlgorithm = context.actorOf(VoluntaryLeaveAlgorithm.props(nodeInfo))
      voluntaryLeaveAlgorithm ! VoluntaryLeaveStart
    case VoluntaryLeaveOk =>
//      log.info("Voluntary departure procedure complete. Shutting down node: {}", nodeId)
      context.stop(pointersRef)
      context.stop(self)
    case ClosestPrecedingNodeAlgorithm.Ping =>
      sender() ! ClosestPrecedingNodeAlgorithm.Pong
  }

  override def receive: Receive = {
    case RegisterOk(_) =>
      val newPointersRef = newPointers(nodeId, nodeInfo)

      val newCheckPredecessorAlgorithmRef = newCheckPredecessorAlgorithm(newPointersRef)
      val newCheckPredecessorCancellable = scheduleCheckPredecessor(newCheckPredecessorAlgorithmRef)

      val newStabilisationAlgorithmRef = newStabilisationAlgorithm(newPointersRef)
      val newStabilisationCancellable = scheduleStabilisation(newStabilisationAlgorithmRef)

      val newFixFingersAlgorithmRef = newFixFingersAlgorithm(newPointersRef)
      val newFixFingersCancellable = scheduleFixFingers(newFixFingersAlgorithmRef)

      context.become(
        receiveWhileReady(
          newPointersRef,
          newCheckPredecessorAlgorithmRef,
          newCheckPredecessorCancellable,
          newStabilisationAlgorithmRef,
          newStabilisationCancellable,
          newFixFingersAlgorithmRef,
          newFixFingersCancellable
        ))

      unstashAll()

    case _ =>
      stash()
  }
}

object Node {

  sealed trait Request

  sealed trait Response

  final case class ClosestPrecedingNode(queryId: Long) extends Request

  sealed trait ClosestPrecedingNodeResponse extends Response

  final case class ClosestPrecedingNodeOk(node: NodeInfo) extends ClosestPrecedingNodeResponse

  final case class ClosestPrecedingNodeError(message: String) extends ClosestPrecedingNodeResponse

  final case class FindPredecessor(queryId: Long, lookupId: Option[Long] = None) extends Request

  sealed trait FindPredecessorResponse extends Response

  final case class FindPredecessorOk(queryId: Long, predecessor: NodeInfo) extends FindPredecessorResponse

  final case class FindPredecessorError(queryId: Long, message: String) extends FindPredecessorResponse

  final case class FindSuccessor(queryId: Long, lookupId: Option[Long] = None) extends Request

  sealed trait FindSuccessorResponse extends Response

  final case class FindSuccessorOk(queryId: Long, successor: NodeInfo) extends FindSuccessorResponse

  final case class FindSuccessorError(queryId: Long, message: String) extends FindSuccessorResponse

  final case class Join(seedId: Long, seedAddr: InetSocketAddress, seedRef: ActorRef) extends Request

  sealed trait JoinResponse extends Response

  case object JoinOk extends JoinResponse

  final case class JoinError(message: String) extends JoinResponse

  final case class Notify(nodeId: Long, nodeAddr: InetSocketAddress, nodeRef: ActorRef) extends Request

  sealed trait NotifyResponse extends Response

  case object NotifyOk extends NotifyResponse

  case object NotifyIgnored extends NotifyResponse

  final case class NotifyError(message: String) extends NotifyResponse

  case object Terminate extends Request

  sealed trait TerminateResponse extends Response

  case object TerminateOk extends TerminateResponse

  def props(nodeId: Long,
            nodeAddr: InetSocketAddress,
            config: ChordConfig,
            eventStream: EventStream,
            router: ActorRef): Props =
    Props(new Node(nodeId, nodeAddr, config, eventStream, router))
}
