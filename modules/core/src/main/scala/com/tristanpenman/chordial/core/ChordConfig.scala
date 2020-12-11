package com.tristanpenman.chordial.core
import akka.util.Timeout

import scala.concurrent.duration.FiniteDuration

final case class ChordConfig(keyspaceBits: Int,
                             algorithmTimeout: Timeout,
                             externalRequestTimeout: Timeout,
                             checkPredecessorDelay: FiniteDuration,
                             checkPredecessorTimeout: Timeout,
                             stabilizationDelay: FiniteDuration,
                             stabilizationTimeout: Timeout,
                             fixFingersDelay: FiniteDuration,
                             fixFingersTimeout: Timeout
                            )
