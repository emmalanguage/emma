package eu.stratosphere.emma.streaming.backend.api

/**
  * This is an example for the target language.
  */
object Example {

  trait GetSetState[S] {
    def get(): S

    def set(s: S): Unit
  }

  class VarGetSetState[S](private var s: S) extends GetSetState[S] {
    override def get(): S = s

    override def set(s: S): Unit = {
      this.s = s
    }
  }

  def main(args: Array[String]): Unit = {

    // initialize DAG
    val dag: DAG = new MockDAG()

    // add integer source that does nothing
    dag.addSource("src1", new Source[Unit, Int](
      new SourceHandler[Unit, Int] {
        override def invoke(collector: Collector[Int], state: Unit): Unit = {}
      },
      parallelism = 2,
      initState = ()
    ))

    // add string source that does nothing
    dag.addSource("src2", new Source[Unit, String](
      new SourceHandler[Unit, String] {
        override def invoke(collector: Collector[String], state: Unit): Unit = {}
      },
      parallelism = 3,
      initState = ()
    ))

    // handle integer source, count incoming values
    val inpHandler1 = new InputHandler[GetSetState[Int], Int, String]("src1",
      new Handler[GetSetState[Int], Int, String] {
        override def process(in: Int, collector: Collector[String], state: GetSetState[Int]): Unit = {
          val cnt = state.get()
          state.set(cnt + 1)

          collector.emit("#" + cnt + ": NUM " + in.toString)
        }
      },
      new Handler[GetSetState[Int], MetaMessage, MetaMessage] {
        override def process(in: MetaMessage, collector: Collector[MetaMessage], state: GetSetState[Int]): Unit = ???
      },
      deserializer = null,
      serializer = null
    )

    // handle string source, count incoming values
    val inpHandler2 = new InputHandler[GetSetState[Int], String, String]("src2",
      new Handler[GetSetState[Int], String, String] {
        override def process(in: String, collector: Collector[String], state: GetSetState[Int]): Unit = {
          val cnt = state.get()
          state.set(cnt + 1)

          collector.emit("#" + cnt + ": STR " + in)
        }
      },
      new Handler[GetSetState[Int], MetaMessage, MetaMessage] {
        override def process(in: MetaMessage, collector: Collector[MetaMessage], state: GetSetState[Int]): Unit = ???
      },
      deserializer = null,
      serializer = null
    )

    // add an operator that takes the 2 sources and counts incoming values
    dag.addOperator("op1",
      new Operator[GetSetState[Int], String](Seq(inpHandler1, inpHandler2), 4, new VarGetSetState[Int](0)))

    // execute job
    dag.execute()
  }
}
