package com.tristanpenman.chordial.core

sealed trait Event

object Event {

  final case class FingerReset(nodeId: Long, index: Int) extends Event

  final case class FingerUpdated(nodeId: Long, index: Int, fingerId: Long) extends Event

  final case class NodeCreated(nodeId: Long, successorId: Long) extends Event

  final case class NodeShuttingDown(nodeId: Long) extends Event

  final case class PredecessorReset(nodeId: Long) extends Event

  final case class PredecessorUpdated(nodeId: Long, predecessorId: Long) extends Event

  final case class SuccessorUpdated(nodeId: Long, successorId: Long) extends Event

  final case class LookupStarted(nodeId: Long, queryId: Long, lookupId: Long) extends Event

  final case class NodeVisitedByLookup(nodeId: Long, queryId: Long, lookupId: Long) extends Event

  final case class LookupCompleted(nodeId: Long, queryId: Long, successorId: Long, lookupId: Long) extends Event

}
