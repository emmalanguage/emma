package eu.stratosphere.emma.streaming.backend.api

/**
  * Method to read input. The method [[invoke]] gets called once.
  *
  * @tparam S
  * Type of source state.
  * @tparam OUT
  * Type of source output.
  */
trait SourceHandler[S, OUT] {
  def invoke(collector: Collector[OUT], state: S): Unit
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
  * @tparam OUT
  * Type of source output.
  */
case class Source[S, OUT](handler: SourceHandler[S, OUT], parallelism: Int, initState: S)
