package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorLogging, ActorRef, Props, ReceiveTimeout}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers.{GetFingerTable, GetFingerTableOk}
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

import scala.concurrent.duration.Duration
import scala.util.control.Breaks._

/**
  * Actor class that implements a simplified version of the ClosestPrecedingNode algorithm
  *
  * The ClosestPrecedingNode algorithm is defined in the Chord paper as follows:
  *
  * {{{
  *   n.closest_preceding_node(id)
  *     for i - m downto 1
  *       if (finger[i].node IN (n, id))
  *         return finger[i].node;
  *     return n;
  * }}}
  *
  * The algorithm implemented here behaves as though the node has a finger table of size 2, with the first entry being
  * the node's successor, and the second entry being the node itself.
  */
final class ClosestPrecedingNodeAlgorithm(node: NodeInfo,
                                          pointersRef: ActorRef,
                                          fingerTableSize: Int,
                                          extTimeout: Timeout)
    extends Actor
    with ActorLogging {

  import ClosestPrecedingNodeAlgorithm._

  private def reset = {
    context.setReceiveTimeout(Duration.Undefined)
    context.become(receive)
  }

  private def awaitFinger(queryId: Long, replyTo: ActorRef): Receive = {
    case GetFingerTableOk(fingerTable) =>
      var done = false
      breakable
      {
        for (k <- fingerTable.size - 1 to 0 by -1) {
          val interval = Interval(node.id, queryId, inclusiveBegin = false, inclusiveEnd = false)
          if (interval.contains(fingerTable(k).id)) {
//            log.info(s"[queryId: $queryId] - Send ClosestPrecedingNodeAlgorithmFinished(${fingerTable(k).id}) (in loop)")
            replyTo ! ClosestPrecedingNodeAlgorithmFinished(fingerTable(k))
//            reset
                      context.stop(self)
            done = true
            break
          }
        }
      }
      if (!done) {
//        log.info(s"[queryId: $queryId] - Send ClosestPrecedingNodeAlgorithmFinished(${node.id}) (after loop)")
        replyTo ! ClosestPrecedingNodeAlgorithmFinished(node)
//                reset
        context.stop(self)
      }

    case ClosestPrecedingNodeAlgorithmStart(_) =>
      sender() ! ClosestPrecedingNodeAlgorithmAlreadyRunning

//    case ReceiveTimeout =>
//      reset
//      replyTo ! ClosestPrecedingNodeAlgorithmError(s"{node.id}.ClosestPrecedingNode($queryId) timed out")
  }

  override def receive: Receive = {
    case ClosestPrecedingNodeAlgorithmStart(queryId) =>
      pointersRef ! GetFingerTable
      context.become(awaitFinger(queryId, sender()))
//      context.setReceiveTimeout(extTimeout.duration)
  }
}

object ClosestPrecedingNodeAlgorithm {

  sealed trait ClosestPrecedingNodeAlgorithmRequest

  final case class ClosestPrecedingNodeAlgorithmStart(queryId: Long) extends ClosestPrecedingNodeAlgorithmRequest

  sealed trait ClosestPrecedingNodeAlgorithmStartResponse

  final case class ClosestPrecedingNodeAlgorithmFinished(finger: NodeInfo)
      extends ClosestPrecedingNodeAlgorithmStartResponse

  case object ClosestPrecedingNodeAlgorithmAlreadyRunning extends ClosestPrecedingNodeAlgorithmStartResponse

  final case class ClosestPrecedingNodeAlgorithmError(message: String)
      extends ClosestPrecedingNodeAlgorithmStartResponse

  sealed trait ClosestPrecedingNodeAlgorithmResetResponse

  case object ClosestPrecedingNodeAlgorithmReady extends ClosestPrecedingNodeAlgorithmResetResponse

  def props(node: NodeInfo, pointersRef: ActorRef, fingerTableSize: Int, extTimeout: Timeout): Props =
    Props(new ClosestPrecedingNodeAlgorithm(node, pointersRef, fingerTableSize, extTimeout: Timeout))

}
