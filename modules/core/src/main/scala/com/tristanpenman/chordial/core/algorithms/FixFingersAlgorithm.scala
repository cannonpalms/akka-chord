package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorLogging, ActorRef, Props, ReceiveTimeout}
import com.tristanpenman.chordial.core.Node._
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.shared.NodeInfo

import scala.concurrent.duration._

/**
  * Actor class that implements the FixFingers algorithm
  *
  * The FixFingers algorithm is defined in the Chord paper as follows:
  *
  * {{{
  *   n.fix_fingers()
  *     next = next + 1;
  *     if (next > m)
  *       next = 1;
  *     finger[next] = find_successor(n + 2^(next-1))
  * }}}
  *
  * Although the algorithm is defined a way that allows 'find_successor' to be performed as an ordinary method call,
  * this class performs the operation by sending a message to an ActorRef and awaiting a response.
  */
final class FixFingersAlgorithm(node: NodeInfo, pointersRef: ActorRef, keyspaceBits: Int)
    extends Actor
    with ActorLogging {

  import FixFingersAlgorithm._

  private val idModulus = 1 << keyspaceBits

  def awaitIncrementNextFingerToFix(replyTo: ActorRef): Receive = {
    case IncrementNextFingerToFixOk =>
      replyTo ! FixFingersAlgorithmOk
      context.become(receive)

    case FixFingersAlgorithmStart =>
      sender() ! FixFingersAlgorithmAlreadyRunning

    case message =>
      log.warning("Received unexpected message while waiting for IncrementNextFingerToFixResponse: {}", message)
  }

  def awaitUpdateFinger(replyTo: ActorRef): Receive = {
    case UpdateFingerOk =>
      pointersRef ! IncrementNextFingerToFix
      context.become(awaitIncrementNextFingerToFix(replyTo))

    case UpdateFingerInvalidIndex =>
      replyTo ! FixFingersAlgorithmError("UpdateFinger request failed with UpdateFingerInvalidIndex.")
      context.become(receive)

    case FixFingersAlgorithmStart =>
      sender() ! FixFingersAlgorithmAlreadyRunning

    case message =>
      log.warning("Received unexpected message while waiting for UpdateFingerResponse: {}", message)
  }
  def awaitFindSuccessor(replyTo: ActorRef, next: Int): Receive = {
    case FindSuccessorOk(_, successor) =>
//       log.info(s"UpdateFinger($next -> ${successor.id})")
      pointersRef ! UpdateFinger(next, successor)
      context.setReceiveTimeout(Duration.Undefined)
      context.become(awaitUpdateFinger(replyTo))

    case FindSuccessorError(queryId, message) =>
      replyTo ! FixFingersAlgorithmError(
        s"FindSuccessor request (node: ${node.id}, query: $queryId) failed with message: $message")
      context.become(receive)

    case FixFingersAlgorithmStart =>
      sender() ! FixFingersAlgorithmAlreadyRunning

    case ReceiveTimeout =>
      replyTo ! FixFingersAlgorithmError(
        s"Lookup request through node ${node.id} failed."
      )
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)

    case message =>
      log.warning("Received unexpected message while waiting for FindSuccessorResponse: {}", message)
  }

  def awaitGetNextFingerToFix(replyTo: ActorRef): Receive = {
    case GetNextFingerToFixOk(next) =>
      val nextFingerId = (node.id + (1 << next)) % idModulus
//       log.info(s"Next finger to fix: $next (id: $nextFingerId) (nodeId: ${node.id}, idModulus: $idModulus)")
//       log.info(s"FindSuccessor($nextFingerId)")
      node.ref ! FindSuccessor(nextFingerId)
      context.setReceiveTimeout(50.millis)
      context.become(awaitFindSuccessor(replyTo, next))

    case FixFingersAlgorithmStart =>
      sender() ! FixFingersAlgorithmAlreadyRunning

    case ReceiveTimeout =>
      replyTo ! FixFingersAlgorithmError("GetNextFingerToFix timed out.")
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)

    case message =>
      log.warning("Received unexpected message while waiting for NextFingerToFixResponse: {}", message)
  }

  override def receive: Receive = {
    case FixFingersAlgorithmStart =>
      pointersRef ! GetNextFingerToFix
      context.become(awaitGetNextFingerToFix(sender()))

    case message =>
      log.warning("Received unexpected message while waiting for FixFingersAlgorithmStart: {}", message)
  }
}

object FixFingersAlgorithm {

  case object FixFingersAlgorithmStart

  sealed trait FixFingersAlgorithmStartResponse

  case object FixFingersAlgorithmOk extends FixFingersAlgorithmStartResponse

  case object FixFingersAlgorithmAlreadyRunning extends FixFingersAlgorithmStartResponse

  final case class FixFingersAlgorithmError(message: String) extends FixFingersAlgorithmStartResponse

  def props(node: NodeInfo, pointersRef: ActorRef, keyspaceBits: Int): Props =
    Props(new FixFingersAlgorithm(node, pointersRef, keyspaceBits))
}
