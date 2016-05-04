package eu.stratosphere.emma.streaming.backend.api

/**
  * Message handler with passed state.
  *
  * @tparam S
  * Type of state.
  * @tparam IN
  * Type of input.
  * @tparam OUT
  * Type of output.
  */
trait Handler[S, IN, OUT] {
  def process(in: IN, collector: Collector[OUT], state: S): Unit
}
