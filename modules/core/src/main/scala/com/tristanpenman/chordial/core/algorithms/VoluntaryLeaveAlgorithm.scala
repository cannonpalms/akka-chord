package com.tristanpenman.chordial.core.algorithms
import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers.{GetPredecessor, _}
import com.tristanpenman.chordial.core.algorithms.VoluntaryLeaveAlgorithm.{VoluntaryLeaveOk, VoluntaryLeaveStart}
import com.tristanpenman.chordial.core.shared.NodeInfo

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


final class VoluntaryLeaveAlgorithm(node: NodeInfo) extends Actor with ActorLogging {
  implicit val timeout: Timeout = 10.millis

  private def getPredecessor(node: NodeInfo) =
    node.ref
      .ask(GetPredecessor)
      .pipeTo(self)

  private def getSuccessor(node: NodeInfo) =
    node.ref
      .ask(GetSuccessor)
      .pipeTo(self)


  private def informNeighbors(predecessorInformed: Boolean, successorInformed: Boolean): Receive = {
    case UpdatePredecessorOk | ResetPredecessorOk =>
      if (predecessorInformed) {
        node.ref ! VoluntaryLeaveOk
        context.stop(self)
      } else {
        context.become(informNeighbors(predecessorInformed, true))
      }
    case UpdateSuccessorOk =>
      if (successorInformed) {
        node.ref ! VoluntaryLeaveOk
        context.stop(self)
      } else {
        context.become(informNeighbors(true, successorInformed))
      }
  }

  private def inform(predecessor: Option[NodeInfo], successor: NodeInfo) = {
    if (predecessor.nonEmpty) {
      predecessor.get.ref
        .ask(UpdateSuccessor(successor))
        .pipeTo(self)
      successor.ref
        .ask(UpdatePredecessor(predecessor.get))
        .pipeTo(self)
      context.become(informNeighbors(false, false))
    } else {
      successor.ref
        .ask(ResetPredecessor)
        .pipeTo(self)
      context.become(informNeighbors(true, false))
    }

  }

  private def awaitNeighbors(predecessor: Option[Option[NodeInfo]], successor: Option[NodeInfo]): Receive = {
    case GetPredecessorOk(p) =>
      if (successor.nonEmpty)
        inform(Some(p), successor.get)
      else
        context.become(awaitNeighbors(Some(Some(p)), successor))
    case GetPredecessorOkButUnknown  =>
      if (successor.nonEmpty)
        inform(None, successor.get)
      else
        context.become(awaitNeighbors(Some(None), successor))
    case GetSuccessorOk(s) =>
      if (predecessor.nonEmpty)
        inform(predecessor.get, s)
      else
        context.become(awaitNeighbors(predecessor, Some(s)))
  }

  override def receive: Receive = {
    case VoluntaryLeaveStart =>
//      log.info("Beginning voluntary departure procedure for node: {}", node.id)
      context.become(awaitNeighbors(None, None))
      getPredecessor(node)
      getSuccessor(node)

  }
}

object VoluntaryLeaveAlgorithm {
  sealed trait VoluntaryLeaveRequest
  case object VoluntaryLeaveStart extends VoluntaryLeaveRequest

  sealed trait VoluntaryLeaveResponse
  case object VoluntaryLeaveOk extends VoluntaryLeaveResponse
  final case class VoluntaryLeaveError(message: String) extends VoluntaryLeaveResponse

  def props(node: NodeInfo): Props =
    Props(new VoluntaryLeaveAlgorithm(node))
}
