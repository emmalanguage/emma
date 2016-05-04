package eu.stratosphere.emma.streaming.backend.api

/**
  * Dataflow directed acyclic graph. Backends (Flink, Storm etc.) need to extend this.
  * A node in the graph can be a [[Source]] without input edges, or an [[Operator]] with input edges.
  */
trait DAG {

  /**
    * Add a source node to the [[DAG]].
    *
    * @param name
    * @param src
    * @tparam S
    * @tparam T
    */
  def addSource[S, T](name: String, src: Source[S, T]): Unit

  /**
    * Add an [[Operator]] node to the [[DAG]]
    *
    * @param name
    * Name of the operator.
    * @param op
    * Operator.
    * @tparam S
    * Type of operator state.
    * @tparam T
    * Type of operator output.
    */
  def addOperator[S, T](name: String, op: Operator[S, T]): Unit

  /**
    * Execute the job represented by the [[DAG]].
    */
  def execute(): Unit

}
