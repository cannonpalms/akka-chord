package com.tristanpenman.chordial.demo

import akka.actor.{Actor, ActorLogging, Props}
import com.tristanpenman.chordial.core.Event.{LookupCompleted, LookupStarted, NodeVisitedByLookup}

final class SimulationTracker extends Actor with ActorLogging {

  private def receiveWhileReady(lookups: Map[Long, Int]): Receive = {
    case LookupStarted(nodeId, queryId, lookupId) =>
      context.become(receiveWhileReady(lookups + (lookupId -> 0)))
    case LookupCompleted(nodeId, queryId, successorId, lookupId) =>
      val pathLength = lookups(lookupId)
      log.info(s"""Lookup completed: { "nodeId": $nodeId, "queryId": $queryId, "successorId": $successorId } = $pathLength""")
    case NodeVisitedByLookup(nodeId, queryId, lookupId) =>
      context.become(receiveWhileReady(lookups.updatedWith(lookupId)({
        case Some(count) => Some(count + 1)

        // if we are receiving visit events before a lookup started event,
        // that should probably be a red flag, but oh well
        case None => Some(1)
      })))
  }

  override def receive: Receive =
    receiveWhileReady(Map.empty)
}

object SimulationTracker {
  def props: Props = Props(new SimulationTracker)
}
