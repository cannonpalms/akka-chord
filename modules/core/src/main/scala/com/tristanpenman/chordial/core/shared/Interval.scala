package com.tristanpenman.chordial.core.shared

final class Interval(begin: Long, end: Long, inclusiveBegin: Boolean, inclusiveEnd: Boolean) {
  def contains(id: Long): Boolean =
    if (begin < end) {
      ((inclusiveBegin && id >= begin || !inclusiveBegin && id > begin)
      && (inclusiveEnd && id <= end || !inclusiveEnd && id < end))
      // Interval does not wrap around
    } else {
      ((inclusiveBegin && id >= begin || !inclusiveBegin && id > begin)
      || (inclusiveEnd && id <= end || !inclusiveEnd && id < end))
//      id >= begin || id < end // Interval is empty (begin == end) or wraps back to Long.MinValue (begin > end)
    }
}

object Interval {
  def apply(begin: Long, end: Long, inclusiveBegin: Boolean, inclusiveEnd: Boolean): Interval =
    new Interval(begin, end, inclusiveBegin, inclusiveEnd)
}
