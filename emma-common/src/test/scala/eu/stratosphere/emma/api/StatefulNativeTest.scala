package eu.stratosphere.emma.api

import eu.stratosphere.emma.api.model.{Identity, id}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatefulNativeTest extends FlatSpec with Matchers with BeforeAndAfter {

  "Stateful native" should "do deep copies" in  {

    val b1 = DataBag(Seq(Foo(1, 1)))

    val s1 = stateful[Foo, Int](b1)
    s1.updateWithZero(x => {
      x.n = x.n + 1
      DataBag()
    })
    b1.fetch().head should equal (Foo(1, 1)) // ctor did deep copy, original should not change
    val b2 = s1.bag()
    b2.fetch().head should equal (Foo(1, 2)) // we see the change in the stateful
    s1.updateWithZero(x => {
      x.n = x.n + 1
      DataBag()
    })
    b2.fetch().head should equal (Foo(1, 2)) // .bag() did deep copy, the resulting DataBag is independent of the stateful
  }
}


case class Foo(@id s: Int, var n: Int) extends Identity[Int] {
  override def identity = n
}
