package eu.stratosphere.emma.macros

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import org.junit.runner.RunWith
import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks

@RunWith(classOf[JUnitRunner])
class SemanticChecksTest extends FlatSpec with PropertyChecks with Matchers {
  import CheckForStatefulAccessedFromUdfTest.Foo

  "The updateWith* functions" should "throw an exception when the stateful is accessed from the updateWith* UDF" in {
    val b = DataBag(Seq(Foo(1, 1)))

    // These should throw exceptions:

    intercept[StatefulAccessedFromUdfException] {
      val s = stateful[Foo, Int](b)
      s.updateWithZero(x => s.bag())
    }

    val us = DataBag(Seq(Foo(1, 2)))

    intercept[StatefulAccessedFromUdfException] {
      val s = stateful[Foo, Int](b)
      s.updateWithOne(us)(_.s, (x,y) => s.bag())
    }
    intercept[StatefulAccessedFromUdfException] {
      val s = stateful[Foo, Int](b)
      s.updateWithMany(us)(_.s, (x,y) => s.bag())
    }

    // And these should not throw exceptions:
    val s = stateful[Foo, Int](b)
    s.updateWithZero(x => DataBag())
    s.updateWithOne(us)(_.s, (x, y) => DataBag())
    s.updateWithMany(us)(_.s, (x, y) => DataBag())
  }

  "parallelize" should "check for the stateful being accessed from the updateWith* UDF" in {
    val b = DataBag(Seq(Foo(1, 1)))

    """emma.parallelize {
      val s = stateful[Foo, Int](b)
      s.updateWithZero(x => s.bag())
    }""" shouldNot compile

    """emma.parallelize {
      val s = stateful[Foo, Int](b)
      s.updateWithOne(us)(_.s, (x,y) => s.bag())
    }""" shouldNot compile

    """emma.parallelize {
      val s = stateful[Foo, Int](b)
      s.updateWithMany(us)(_.s, (x,y) => s.bag())
    }""" shouldNot compile

    // This should compile
    val dummy = emma.parallelize {
      val s = stateful[Foo, Int](b)
      val us = DataBag(Seq(Foo(1, 2)))
      s.updateWithZero(x => DataBag())
      s.updateWithOne(us)(_.s, (x, y) => DataBag())
      s.updateWithMany(us)(_.s, (x, y) => DataBag())
    }
  }
}

object CheckForStatefulAccessedFromUdfTest {
  case class Foo(@id s: Int, var n: Int) extends Identity[Int] {
    override def identity = n
  }
}
