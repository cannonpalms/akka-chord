package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.core.Node._
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Actor class that implements the Stabilise algorithm
  *
  * == Algorithm ==
  *
  * The basic stabilisation algorithm is defined in the Chord paper as follows:
  *
  * {{{
  *   n.stabilise()
  *     x = successor.predecessor
  *     if (x IN (n, successor))
  *       successor = x;
  *     successor.notify(n);
  * }}}
  *
  * This implementation includes enhancements described in section E.3 (Failure and Replication) of the Chord paper,
  * that relate to maintaining a list of successor pointers that can be used if one or more of the closest successors
  * fails to respond within a reasonable amount of time. After the closest successor has been determined, its successor
  * list is reconciled with that of the current node.
  *
  * == Messaging semantics ==
  *
  * This actor essentially functions as a state machine for the execution state of the 'stabilisation' algorithm.
  *
  * The actor is either in the 'running' state or the 'ready' state. Sending a \c StabilisationAlgorithmReset message
  * at any time will result in a transition to the 'ready' state, with a new set of arguments. However this will not
  * stop existing invocations of the algorithm from running to completion. \c StabilisationAlgorithmReset messages are
  * idempotent, and will always result in a \c StabilisationAlgorithmResetOk message being returned to the sender.
  *
  * The actor is initially in the 'ready' state, using the arguments provided at construction time.
  *
  * Sending a \c StabilisationAlgorithmStart message will start the algorithm, but only while in the 'ready' state.
  * When the algorithm is in the running state, a \c StabilisationAlgorithmAlreadyRunning message will be returned to
  * the sender. This allows for a certain degree of back-pressure in the client.
  *
  * When the algorithm completes, a \c StabilisationAlgorithmFinished or \c StabilisationAlgorithmError message will be
  * sent to the original sender, depending on the outcome.
  */
final class StabilisationAlgorithm(initialNode: NodeInfo, initialPointersRef: ActorRef, initialRequestTimeout: Timeout)
    extends Actor
    with ActorLogging {

  import StabilisationAlgorithm._

  /**
    * Execute the 'stabilisation' algorithm asynchronously
    *
    * @param node current node
    * @param pointersRef current node's network pointer data
    * @param requestTimeout time to wait on requests to external resources
    *
    * @return a \c Future that will complete once the updated successor has been notified of the current node
    */
  private def runAsync(node: NodeInfo, pointersRef: ActorRef, requestTimeout: Timeout): Future[Unit] =
    pointersRef
      .ask(GetSuccessor)(requestTimeout)
      .mapTo[GetSuccessorResponse]
      .flatMap {
        case GetSuccessorOk(successor) =>
          successor.ref
            .ask(GetPredecessor)(requestTimeout)
            .map(m => (m, successor))
      }
      .mapTo[(GetPredecessorResponse, NodeInfo)]
      .map {
        case (GetPredecessorOk(candidate), currentSuccessor)
            if Interval(node.id + 1, currentSuccessor.id).contains(candidate.id) =>
          candidate
        case (GetPredecessorOk(_), currentSuccessor) =>
          currentSuccessor
        case (GetPredecessorOkButUnknown, currentSuccessor) =>
          currentSuccessor
      }
      .flatMap { newSuccessor =>
        pointersRef
          .ask(UpdateSuccessor(newSuccessor))(requestTimeout)
          .mapTo[UpdateSuccessorResponse]
          .map {
            case UpdateSuccessorOk =>
              newSuccessor
          }
      }
      .flatMap { newSuccessor =>
        newSuccessor.ref
          .ask(Notify(node.id, node.ref))(requestTimeout)
          .mapTo[NotifyResponse]
          .map {
            case NotifyOk | NotifyIgnored =>
            case NotifyError(message)     => throw new Exception(message)
          }
      }

  private def running(): Receive = {
    case StabilisationAlgorithmStart =>
      sender() ! StabilisationAlgorithmAlreadyRunning

    case StabilisationAlgorithmReset(newNode, newPointersRef, newRequestTimeout) =>
      context.become(ready(newNode, newPointersRef, newRequestTimeout))
      sender() ! StabilisationAlgorithmReady
  }

  private def ready(node: NodeInfo, pointersRef: ActorRef, requestTimeout: Timeout): Receive = {
    case StabilisationAlgorithmStart =>
      val replyTo = sender()
      runAsync(node, pointersRef, requestTimeout).onComplete {
        case util.Success(()) =>
          replyTo ! StabilisationAlgorithmFinished
        case util.Failure(exception) =>
          replyTo ! StabilisationAlgorithmError(exception.getMessage)
      }
      context.become(running())

    case StabilisationAlgorithmReset(newNode, newPointersRef, newRequestTimeout) =>
      context.become(ready(newNode, newPointersRef, newRequestTimeout))
      sender() ! StabilisationAlgorithmReady
  }

  override def receive: Receive =
    ready(initialNode, initialPointersRef, initialRequestTimeout)
}

object StabilisationAlgorithm {

  sealed trait StabilisationAlgorithmRequest

  case object StabilisationAlgorithmStart extends StabilisationAlgorithmRequest

  final case class StabilisationAlgorithmReset(newNode: NodeInfo, newPointersRef: ActorRef, newRequestTimeout: Timeout)
      extends StabilisationAlgorithmRequest

  sealed trait StabilisationAlgorithmStartResponse

  case object StabilisationAlgorithmFinished extends StabilisationAlgorithmStartResponse

  case object StabilisationAlgorithmAlreadyRunning extends StabilisationAlgorithmStartResponse

  final case class StabilisationAlgorithmError(message: String) extends StabilisationAlgorithmStartResponse

  sealed trait StabilisationAlgorithmResetResponse

  case object StabilisationAlgorithmReady extends StabilisationAlgorithmResetResponse

  def props(initialNode: NodeInfo, initialPointersRef: ActorRef, initialRequestTimeout: Timeout): Props =
    Props(new StabilisationAlgorithm(initialNode, initialPointersRef, initialRequestTimeout))
}
