package eu.stratosphere.emma.streaming.backend.api

/**
  * Collector to emit output values in an [[Operator]].
 *
  * @tparam OUT Type of output.
  */
trait Collector[OUT] {
  def emit(out: OUT): Unit
}
