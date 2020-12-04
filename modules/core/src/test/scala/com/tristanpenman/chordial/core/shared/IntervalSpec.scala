package com.tristanpenman.chordial.core.shared

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import org.scalatest.WordSpecLike

import scala.concurrent.ExecutionContextExecutor

final class IntervalSpec extends TestKit(ActorSystem("IntervalSpec")) with WordSpecLike with ImplicitSender {

  import IntervalSpec._

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  def intervalActor(begin: Long, end: Long, inclusiveBegin: Boolean, inclusiveEnd: Boolean): ActorRef =
    TestActorRef(factory = new Actor {
      def failOnReceive: Receive = {
        case m =>
          fail(s"Unexpected message of type: ${m.getClass}")
      }

      override def receive: Receive = {
        case Contains(value) =>
          val ret =
            if (Interval(begin, end, inclusiveBegin, inclusiveEnd).contains(value))
              True
            else
              False
          sender() ! ret
      }
    })

  def expectIntervalContains(begin: Long,
                             end: Long,
                             inclusiveBegin: Boolean,
                             inclusiveEnd: Boolean,
                             value: Long,
                             expectedMsg: Any): Unit = {
    val interval = intervalActor(begin, end, inclusiveBegin, inclusiveEnd)
    interval ! Contains(value)
    expectMsg(expectedMsg)
  }

  "The Interval utility" when {
    "from (0, 5)" should {
      val inclusiveBegin = false
      val inclusiveEnd = false
      "contain values 1-4" in {
        val testCases = 1 to 4
        testCases.foreach { value =>
          expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, value, True)
        }
      }
      "not contain the value 0" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 0, False)
      }
      "contain the value 5" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 5, False)
      }
      "not contain the value 6" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 6, False)
      }
    }

    "from [0, 5)" should {
      val inclusiveBegin = true
      val inclusiveEnd = false
      "contain values 1-4" in {
        val testCases = 1 to 4
        testCases.foreach { value =>
          expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, value, True)
        }
      }
      "not contain the value 0" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 0, True)
      }
      "contain the value 5" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 5, False)
      }
      "not contain the value 6" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 6, False)
      }
    }

    "from (0, 5]" should {
      val inclusiveBegin = false
      val inclusiveEnd = true
      "contain values 1-4" in {
        val testCases = 1 to 4
        testCases.foreach { value =>
          expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, value, True)
        }
      }
      "not contain the value 0" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 0, False)
      }
      "contain the value 5" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 5, True)
      }
      "not contain the value 6" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 6, False)
      }
    }

    "from [0, 5]" should {
      val inclusiveBegin = true
      val inclusiveEnd = true
      "contain values 1-4" in {
        val testCases = 1 to 4
        testCases.foreach { value =>
          expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, value, True)
        }
      }
      "not contain the value 0" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 0, True)
      }
      "contain the value 5" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 5, True)
      }
      "not contain the value 6" in {
        expectIntervalContains(0, 5, inclusiveBegin, inclusiveEnd, 6, False)
      }
    }
  }
}

object IntervalSpec {
  sealed trait Request
  sealed trait Response
  final case class Contains(value: Long) extends Request
  case object True extends Response
  case object False extends Response
}
