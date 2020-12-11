package com.tristanpenman.chordial.core

import akka.actor._
import akka.event.EventStream
import com.tristanpenman.chordial.core.Event._
import com.tristanpenman.chordial.core.shared.NodeInfo

final class Pointers(nodeId: Long, fingerTableSize: Int, seedNode: NodeInfo, eventStream: EventStream)
    extends Actor
    with ActorLogging {

  import Pointers._

  private def newFingerTable = Vector.fill(fingerTableSize) {
    seedNode
  }

  private def receiveWhileReady(successor: NodeInfo,
                                predecessor: Option[NodeInfo],
                                fingerTable: Vector[NodeInfo],
                                next: Int): Receive = {
    case GetId =>
      sender() ! GetIdOk(nodeId)

    case GetPredecessor =>
      predecessor match {
        case Some(info) =>
          sender() ! GetPredecessorOk(info)
        case None =>
          sender() ! GetPredecessorOkButUnknown
      }

    case GetSuccessor =>
      sender() ! GetSuccessorOk(successor)

    case GetNextFingerToFix =>
      sender() ! GetNextFingerToFixOk(next)

    case GetFingerTable =>
      sender() ! GetFingerTableOk(fingerTable)

    case IncrementNextFingerToFix =>
      context.become(receiveWhileReady(successor, predecessor, fingerTable, 1 + next % (fingerTableSize - 1)))
      sender() ! IncrementNextFingerToFixOk

    case ResetFinger(index: Int) =>
      if (index < 0 || index >= fingerTableSize) {
        sender() ! ResetFingerInvalidIndex
      } else {
        context.become(receiveWhileReady(successor, predecessor, fingerTable.updated(index, seedNode), next))
        sender() ! ResetFingerOk
        eventStream.publish(FingerReset(nodeId, index))
      }

    case ResetPredecessor =>
      context.become(receiveWhileReady(successor, None, fingerTable, next))
      sender() ! ResetPredecessorOk
      eventStream.publish(PredecessorReset(nodeId))

    case UpdateFinger(index: Int, finger: NodeInfo) =>
      if (index < 0 || index >= fingerTableSize) {
        sender() ! UpdateFingerInvalidIndex
      } else {
        context.become(receiveWhileReady(successor, predecessor, fingerTable.updated(index, finger), next))
        sender() ! UpdateFingerOk
        eventStream.publish(FingerUpdated(nodeId, index, finger.id))
      }

    case UpdatePredecessor(newPredecessor) =>
      // log.info(s"UpdatePredecessor(${newPredecessor.id})")
      context.become(receiveWhileReady(successor, Some(newPredecessor), fingerTable, next))
      sender() ! UpdatePredecessorOk
      eventStream.publish(PredecessorUpdated(nodeId, newPredecessor.id))

    case UpdateSuccessor(newSuccessor) =>
      // log.info(s"UpdateSuccessor(${newSuccessor.id})")
      if (newSuccessor != successor) {
        context.become(receiveWhileReady(newSuccessor, predecessor, fingerTable.updated(0, newSuccessor), next))
        sender() ! UpdateSuccessorOk
        eventStream.publish(SuccessorUpdated(nodeId, newSuccessor.id))
      } else {
        sender() ! UpdateSuccessorOk
      }
  }

  eventStream.publish(NodeCreated(nodeId, seedNode.id))

  override def receive: Receive =
    receiveWhileReady(seedNode, None, newFingerTable, 1)
}

object Pointers {

  sealed trait Request

  sealed trait Response

  case object GetId extends Request

  sealed trait GetIdResponse extends Response

  final case class GetIdOk(id: Long) extends GetIdResponse

  case object GetPredecessor extends Request

  sealed trait GetPredecessorResponse extends Response

  final case class GetPredecessorOk(predecessor: NodeInfo) extends GetPredecessorResponse

  case object GetPredecessorOkButUnknown extends GetPredecessorResponse

  sealed trait GetSuccessorResponse extends Response

  case object GetSuccessor extends Request

  final case class GetSuccessorOk(successor: NodeInfo) extends GetSuccessorResponse

  final case class GetFingerTable(index: Int) extends Request

  sealed trait GetFingerResponse extends Response

  final case class GetFingerTableOk(fingerTable: Vector[NodeInfo]) extends GetFingerResponse

  case object GetNextFingerToFix extends Request

  sealed trait GetNextFingerToFixResponse extends Response

  final case class GetNextFingerToFixOk(next: Int) extends GetNextFingerToFixResponse

  case object IncrementNextFingerToFix extends Request

  sealed trait IncrementNextFingerToFixResponse extends Response

  case object IncrementNextFingerToFixOk extends IncrementNextFingerToFixResponse

  case object ResetPredecessor extends Request

  sealed trait ResetPredecessorResponse extends Response

  case object ResetPredecessorOk extends ResetPredecessorResponse

  final case class ResetFinger(index: Int) extends Request

  sealed trait ResetFingerResponse extends Response

  case object ResetFingerOk extends ResetFingerResponse

  case object ResetFingerInvalidIndex extends ResetFingerResponse

  final case class UpdateFinger(index: Int, finger: NodeInfo) extends Request

  sealed trait UpdateFingerResponse extends Response

  case object UpdateFingerOk extends UpdateFingerResponse

  case object UpdateFingerInvalidIndex extends UpdateFingerResponse

  final case class UpdatePredecessor(predecessor: NodeInfo) extends Request

  sealed trait UpdatePredecessorResponse extends Response

  case object UpdatePredecessorOk extends UpdatePredecessorResponse

  final case class UpdateSuccessor(successor: NodeInfo) extends Request

  sealed trait UpdateSuccessorResponse extends Response

  case object UpdateSuccessorOk extends UpdateSuccessorResponse

  def props(ownId: Long, keyspaceBits: Int, seed: NodeInfo, eventStream: EventStream): Props =
    Props(new Pointers(ownId, keyspaceBits, seed, eventStream))
}
