package eu.stratosphere.emma.codegen.flink

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model.Identity
import eu.stratosphere.emma.codegen.BaseCodegenTest
import eu.stratosphere.emma.runtime.FlinkLocal
import eu.stratosphere.emma.testutil._
import org.junit.Test

class CodegenTest extends BaseCodegenTest("flink") {

  override def runtimeUnderTest = FlinkLocal("localhost", 6123)

  @Test def testStateful() = {

    // This only works with Flink at the moment, and only locally

    val in = Seq(State(6,8))

    val alg = emma.parallelize {
      val b1 = DataBag(in)
      val s = stateful[State, Long](b1)
      val b2 = s.bag()
      b2.fetch()
    }

    val out = alg.run(rt)
    compareBags(in, out)
  }
}


case class State(id: Long, var value: Int) extends Identity[Long] {
  def identity = id
}