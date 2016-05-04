package eu.stratosphere.emma.streaming.backend.api

/**
  * Input handler, that defines how an operator should handle an input edge in the [[DAG]].
  * An input edge can come from a [[Source]] or another [[Operator]].
  *
  * @param inputName
  * Name of the input operator in the [[DAG]].
  * @param handler
  * Handler of input.
  * @param metaHandler
  * Handler of [[MetaMessage]]s.
  * @param deserializer
  * Deserializer of input messages.
  * @tparam S
  * Type of operator state.
  * @tparam IN
  * Type of input.
  * @tparam OUT
  * Type of output.
  */
case class InputHandler[S, IN, OUT](
                                     inputName: String,
                                     handler: Handler[S, IN, OUT],
                                     metaHandler: Handler[S, MetaMessage, MetaMessage],
                                     deserializer: Serializer[IN]
                                   )

/**
  * Stateful operator that can take multiple type of inputs.
  * Represents a node in a [[DAG]].
  *
  * @param inputs
  * Separate handlers.
  * @param parallelism
  * Number of operator instances to create.
  * @param serializer
  * Serializer of output messages.
  * @param partitioner
  * Partitioner of output values.
  * @param metaMsgPartitioner
  * Partitioner of [[MetaMessage]]s.
  * @param initState
  * Initial operator state.
  * @tparam S
  * Type of state.
  * @tparam OUT
  * Type of output.
  */
case class Operator[S, OUT](inputs: Seq[InputHandler[S, _, OUT]],
                            parallelism: Int,
                            serializer: Serializer[OUT],
                            partitioner: Partitioner[OUT],
                            metaMsgPartitioner: Partitioner[MetaMessage],
                            initState: S)

