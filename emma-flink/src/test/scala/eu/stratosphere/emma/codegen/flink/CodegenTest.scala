package eu.stratosphere.emma.codegen.flink

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model.Identity
import eu.stratosphere.emma.codegen.BaseCodegenTest
import eu.stratosphere.emma.runtime.FlinkLocal
import eu.stratosphere.emma.testutil._
import org.junit.Test

class CodegenTest extends BaseCodegenTest("flink") {

  override def runtimeUnderTest = FlinkLocal("localhost", 6123)

  @Test def testStatefulCreateFetch() = {

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

  @Test def testStatefulUpdateWithMany() = {

    // This only works with Flink at the moment, and only locally

    val in = Seq(State(6, 12), State(3, 4))

    val alg = emma.parallelize {

      val b1 = DataBag(in)
      val s = stateful[State, Long](b1)

      val us1 = DataBag(Seq(Update(3, 5)))
      val o1 = s.updateWithMany(us1)(_.identity, (s, us) => {
        for(
          u <- us
        ) yield {
          s.value += u.inc
          s.value * u.inc
        }
      })

      val us2 = DataBag(Seq(Update(3, 1), Update(6, 2)))
      val o2 = s.updateWithMany(us2)(_.identity, (s, us) => {
        for(
          u <- us
        ) yield {
          s.value += u.inc
          s.value * u.inc
        }
      })

      val b2 = s.bag()
      b2.fetch()
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt)
    val exp = alg.run(native)

    // assert that the result contains the expected values
    compareBags(act, exp)
  }
}


case class State(id: Long, var value: Int) extends Identity[Long] {
  def identity = id
}

case class Update(id: Long, inc: Int) extends Identity[Long] {
  def identity = id
}