package com.tristanpenman.chordial.demo

import akka.actor.{Actor, ActorLogging, Props}
import com.tristanpenman.chordial.core.Event.{LookupCompleted, LookupStarted, NodeVisitedByLookup}
import com.tristanpenman.chordial.demo.SimulationTracker.Lookup

final class SimulationTracker extends Actor with ActorLogging {

  private def receiveWhileReady(lookups: Map[Long, Lookup]): Receive = {
    case LookupStarted(nodeId, queryId, lookupId) =>
      context.become(receiveWhileReady(lookups + (lookupId -> Lookup(nodeId, queryId, lookupId, 0))))
    case LookupCompleted(nodeId, queryId, successorId, lookupId) =>
      val lookup = lookups(lookupId)
      log.info(
        s"""Lookup completed: { "nodeId": $nodeId, "queryId": $queryId, "successorId": $successorId, "pathLength": ${lookup.pathLength} }""")
    case NodeVisitedByLookup(nodeId, queryId, lookupId) =>
      log.info(
        s"""Node visited: { "lookupId": $lookupId, "nodeId": $nodeId, "queryId": $queryId }""")
      context.become(receiveWhileReady(lookups.updatedWith(lookupId)({
        case Some(lookup) => Some(Lookup(lookup.nodeId, lookup.queryId, lookup.lookupId, lookup.pathLength + 1))

        // if we are receiving visit events before a lookup started event,
        // that should probably be a red flag, but oh well
        case None => Some(Lookup(nodeId, queryId, lookupId, 1))
      })))
    case _ =>
  }

  override def receive: Receive =
    receiveWhileReady(Map.empty)
}

object SimulationTracker {
  final case class Lookup(nodeId: Long, queryId: Long, lookupId: Long, pathLength: Int)

  def props: Props = Props(new SimulationTracker)
}
