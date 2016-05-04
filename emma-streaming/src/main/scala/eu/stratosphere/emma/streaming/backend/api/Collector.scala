package eu.stratosphere.emma.streaming.backend.api

/**
 * Collector to emit output values in an [[Operator]].
 *
 * @tparam O Type of output.
 */
trait Collector[O] {
  def emit(out: O): Unit
}
