package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.EventStream
import com.tristanpenman.chordial.core.Event.NodeVisitedByLookup
import com.tristanpenman.chordial.core.Node._
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.Router.Forward
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

/**
  * Actor class that implements the FindPredecessor algorithm
  *
  * The FindPredecessor algorithm is defined in the Chord paper as follows:
  *
  * {{{
  *   n.find_predecessor(id)
  *     n' = n;
  *     while (id NOT_IN (n', n'.successor])
  *       n' = n'.closest_preceding_finger(id);
  *     return n';
  * }}}
  *
  * This algorithm has been implemented as a series of alternating 'successor' and 'closest_preceding_finger'
  * operations, each performed by sending a message to an ActorRef and awaiting an appropriate response.
  *
  * Note that the NOT_IN operator is defined in terms of an interval that wraps around to the minimum value.
  */
final class FindPredecessorAlgorithm(router: ActorRef, eventStream: EventStream, lookupId: Option[Long] = None) extends Actor with ActorLogging {

  import FindPredecessorAlgorithm._

  def awaitGetSuccessor(queryId: Long, delegate: ActorRef, candidate: NodeInfo): Actor.Receive = {
    case GetSuccessorOk(successor: NodeInfo) =>
      // log.info(s"Successor of ${candidate.id}: ${successor.id}")
      // log.info(s"Checking if $queryId is within interval (${candidate.id}, ${successor.id}]")

      // mark node as visited if this process is from a lookup
      if (!lookupId.isEmpty)
        eventStream.publish(NodeVisitedByLookup(candidate.id, queryId, lookupId.get))

      // Check whether the query ID belongs to the candidate node's successor
      if (Interval(candidate.id, successor.id, inclusiveBegin = false, inclusiveEnd = true).contains(queryId)) {
        // log.info("Interval match.")
        // If the query ID belongs to the candidate node's successor, then we have successfully found the predecessor
        delegate ! FindPredecessorAlgorithmOk(candidate)
        context.stop(self)
      } else {
        // log.info(s"No interval match. Finding closest preceding node to $queryId")
        // Otherwise, we need to choose the next node by the asking the current candidate node to return what it knows
        // to be the closest preceding finger for the query ID
        router ! Forward(candidate.id, candidate.addr, ClosestPrecedingNode(queryId))
        context.become(awaitClosestPrecedingNode(queryId, delegate))
      }

    case FindPredecessorAlgorithmStart(_, _) =>
      sender() ! FindPredecessorAlgorithmAlreadyRunning

    case message =>
      log.warning("Received unexpected message while waiting for GetSuccessorResponse: {}", message)
  }

  def awaitClosestPrecedingNode(queryId: Long, delegate: ActorRef): Actor.Receive = {
    case ClosestPrecedingNodeOk(candidate) =>
      // log.info(s"Closest preceding node of $queryId: ${candidate.id}")
      // Now that we have the ID and ActorRef for the next candidate node, we can proceed to the next step of the
      // algorithm. This requires that we locate the successor of the candidate node.
      router ! Forward(candidate.id, candidate.addr, GetSuccessor)
      context.become(awaitGetSuccessor(queryId, delegate, candidate))

    case ClosestPrecedingNodeError(message: String) =>
      delegate ! FindPredecessorAlgorithmError(s"ClosestPrecedingFinder request failed with message: $message")
      context.stop(self)

    case FindPredecessorAlgorithmStart(_, _) =>
      sender() ! FindPredecessorAlgorithmAlreadyRunning

    case message =>
      log.warning("Received unexpected message while waiting for ClosestPrecedingFingerResponse: {}", message)
  }

  override def receive: Receive = {
    case FindPredecessorAlgorithmStart(queryId: Long, initialNode: NodeInfo) =>
      initialNode.ref ! GetSuccessor
      context.become(awaitGetSuccessor(queryId, sender(), initialNode))

    case message =>
      log.warning("Received unexpected message while waiting for FindPredecessorAlgorithmStart: {}", message)
  }
}

object FindPredecessorAlgorithm {

  final case class FindPredecessorAlgorithmStart(queryId: Long, initialNode: NodeInfo)

  sealed trait FindPredecessorAlgorithmStartResponse

  case object FindPredecessorAlgorithmAlreadyRunning extends FindPredecessorAlgorithmStartResponse

  final case class FindPredecessorAlgorithmOk(predecessor: NodeInfo) extends FindPredecessorAlgorithmStartResponse

  final case class FindPredecessorAlgorithmError(message: String) extends FindPredecessorAlgorithmStartResponse

  def props(router: ActorRef, eventStream: EventStream, lookupId: Option[Long] = None): Props =
    Props(new FindPredecessorAlgorithm(router, eventStream, lookupId))

}
