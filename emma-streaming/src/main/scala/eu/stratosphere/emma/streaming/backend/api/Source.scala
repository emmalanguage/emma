package eu.stratosphere.emma.streaming.backend.api

/**
 * Method to read input. The method [[invoke]] gets called once.
 *
 * @tparam S
 * Type of source state.
 * @tparam O
 * Type of source output.
 */
trait SourceHandler[S, O] {
  def invoke(collector: Collector[O], state: S): Unit
}

/**
 * Source that serves as an input.
 * Represents a node in a [[DAG]].
 *
 * @param handler
 * Source reader method.
 * @param parallelism
 * Number of parallel source instances.
 * @param initState
 * Initial source state.
 * @tparam S
 * Type of source state.
 * @tparam O
 * Type of source output.
 */
case class Source[S, O](handler: SourceHandler[S, O], parallelism: Int, initState: S)
