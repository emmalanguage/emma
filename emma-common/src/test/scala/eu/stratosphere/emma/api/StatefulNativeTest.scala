package eu.stratosphere.emma.api

import eu.stratosphere.emma.api.model.{Identity, id}

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatefulNativeTest extends FlatSpec with Matchers {

  "Stateful native" should "do deep copies" in  {
    import StatefulNativeTest._

    val initial = DataBag(Foo(1, 1) :: Nil)
    val withState = stateful[Foo, Int](initial)
    withState updateWithZero { x =>
      x.n += 1
      DataBag()
    }

    // constructor did deep copy, original should not change
    initial.fetch().head should be (Foo(1, 1))
    val result = withState.bag()
    // we see the change in the stateful
    result.fetch().head should be (Foo(1, 2))

    withState updateWithZero { x =>
      x.n += 1
      DataBag()
    }

    // `.bag()` did deep copy, the resulting `DataBag` is independent of the stateful
    result.fetch().head should be (Foo(1, 2))
  }
}

object StatefulNativeTest {
  case class Foo(@id s: Int, var n: Int) extends Identity[Int] {
    override def identity = n
  }
}
