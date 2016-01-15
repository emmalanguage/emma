package eu.stratosphere.emma.macros

import java.io.ByteArrayOutputStream
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.runtime.Native
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks

@RunWith(classOf[JUnitRunner])
class FoldSpec extends FunSuite with Matchers with PropertyChecks {

  lazy val runtime = Native()
  val epsilon = 1e-9

  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(workers = Runtime.getRuntime.availableProcessors)

  test("reduceOption (sum)") {
    forAll { xs: Seq[Int] =>
      val result = emma.parallelize {
        DataBag(xs) reduceOption { _ + _ }
      } run runtime

      if (xs.isEmpty) result should be ('empty)
      else result.get should be (xs.sum)
    }
  }

  test("reduce (sum)") {
    forAll { xs: Seq[Int] =>
      val result = emma.parallelize {
        DataBag(xs).reduce(0) { _ + _ }
      } run runtime

      result should be (xs.sum)
    }
  }

  test("fold (sum length)") {
    forAll { xs: Seq[String] =>
      val result = emma.parallelize {
        DataBag(xs).fold(0)(_.length, _ + _)
      } run runtime

      result should be (xs.map { _.length }.sum)
    }
  }

  test("isEmpty and nonEmpty") {
    forAll { xs: Seq[Int] =>
      val (empty, notEmpty) = emma.parallelize {
        val bag = DataBag(xs)
        (bag.isEmpty, bag.nonEmpty)
      } run runtime

      empty should not equal notEmpty
      if (xs.isEmpty) empty should be (true)
      else notEmpty should be (true)
    }
  }

  test("count and size") {
    forAll { xs: Seq[Int] =>
      val (count, size) = emma.parallelize {
        val bag = DataBag(xs)
        (bag.count(_ => true), bag.size)
      } run runtime

      count  should be (xs count { _ => true })
      size   should be (xs.size)
    }
  }

  test("min and max of empty DataBag") {
    intercept[NoSuchElementException] {
      emma.parallelize {
        DataBag(Seq.empty[Int]).min
      } run runtime
    }

    intercept[NoSuchElementException] {
      emma.parallelize {
        DataBag(Seq.empty[Int]).max
      } run runtime
    }
  }

  test("min and max") {
    forAll { xs: Seq[Int] =>
      whenever(xs.nonEmpty) {
        val (min, max) = emma.parallelize {
          val bag = DataBag(xs)
          (bag.min, bag.max)
        } run runtime

        min should be (xs.min)
        max should be (xs.max)
      }
    }
  }

  test("top and bottom (10)") {
    val n = 10
    forAll { xs: Seq[Int] =>
      val (top, bottom) = emma.parallelize {
        val bag = DataBag(xs)
        (bag top n, bag bottom n)
      } run runtime

      val sorted = xs.sorted
      top    should be (sorted.takeRight(n).reverse)
      bottom should be (sorted take n)
    }
  }

  test("sum and product") {
    forAll { xs: Seq[Int] =>
      val (sum, product) = emma.parallelize {
        val bag = DataBag(xs)
        (bag.sum, bag.product)
      } run runtime

      sum     should be (xs.sum)
      product should be (xs.product)
    }
  }

  test("count (even)") {
    forAll { xs: Seq[Int] =>
      val even = emma.parallelize {
        DataBag(xs) count { _ % 2 == 0 }
      } run runtime

      even should be (xs count { _ % 2 == 0 })
    }
  }

  test("exists (odd)") {
    forAll { xs: Seq[Int] =>
      val hasOdd = emma.parallelize {
        DataBag(xs) exists { _ % 2 != 0 }
      } run runtime

      hasOdd should be (xs exists { _ % 2 != 0 })
    }
  }

  test("forall (positive)") {
    forAll { xs: Seq[Int] =>
      val positive = emma.parallelize {
        DataBag(xs) forall { _ > 0 }
      } run runtime

      positive should be (xs forall { _ > 0 })
    }
  }

  test("find (negative)") {
    forAll { xs: Seq[Int] =>
      val negative = emma.parallelize {
        DataBag(xs) find { _ < 0 }
      } run runtime

      xs find { _ < 0 } match {
        case Some(_) => negative.get should be < 0
        case None    => negative should be ('empty)
      }
    }
  }

  test("fold group fusion") {
    val buffer = new ByteArrayOutputStream()
    Console.withOut(buffer) {
      emma.comprehend {
        for (g <- DataBag(1 to 100) groupBy identity)
          yield g.key -> g.values.size
      }
    }

    buffer.toString.toLowerCase should include ("foldgroup")
  }

  test("sample") {
    forAll(Gen.listOfN(100, arb[Int])) { xs: List[Int] =>
      val rand = emma.parallelize {
        DataBag(xs).sample(7)
      }.run(runtime)

      rand should not contain theSameElementsAs (xs take 7)
    }
  }

  def arb[A: Arbitrary]: Gen[A] =
    implicitly[Arbitrary[A]].arbitrary
}
