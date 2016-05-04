package eu.stratosphere.emma.streaming.backend.api

/**
  * A mock [[DAG]] that does nothing. Only for writing examples.
  */
class MockDAG extends DAG {

  override def addOperator[S, T](name: String, op: Operator[S, T]): Unit = { }

  override def execute(): Unit = { }

  override def addSource[S, T](name: String, src: Source[S, T]): Unit = { }

}
