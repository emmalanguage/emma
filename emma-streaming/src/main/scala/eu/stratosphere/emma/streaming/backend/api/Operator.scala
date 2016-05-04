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
 * @tparam I
 * Type of input.
 * @tparam O
 * Type of output.
 */
case class InputHandler[S, I, O](
    inputName: String,
    handler: Handler[S, I, O],
    metaHandler: Handler[S, MetaMessage, MetaMessage],
    deserializer: Serializer[I])

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
 * @tparam O
 * Type of output.
 */
case class Operator[S, O](inputs: Seq[InputHandler[S, _, O]],
    parallelism: Int,
    serializer: Serializer[O],
    partitioner: Partitioner[O],
    metaMsgPartitioner: Partitioner[MetaMessage],
    initState: S)

