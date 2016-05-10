package eu.stratosphere.emma.streaming.backend.api

/**
 * Message handler with passed state.
 *
 * @tparam S
 * Type of state.
 * @tparam I
 * Type of input.
 * @tparam O
 * Type of output.
 */
trait Handler[S, I, O] {
  def process(in: I,
    collector: Collector[O],
    metaMsgCollector: Collector[MetaMessage],
    state: S): Unit
}
