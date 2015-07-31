package eu.stratosphere.emma.macros

import java.io.ByteArrayOutputStream

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.runtime.Native
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks

@RunWith(classOf[JUnitRunner])
class FoldSpec extends FunSuite with Matchers with PropertyChecks {

  val runtime = Native()
  val epsilon = 1e-9

  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(workers = 2)

  test("fold1 (sum)") {
    forAll { xs: Seq[Int] =>
      val result = emma.parallelize {
        DataBag(xs) fold1 { _ + _ }
      } run runtime

      if (xs.isEmpty) result should be ('empty)
      else result.get should be (xs.sum)
    }
  }

  test("fold2 (sum)") {
    forAll { xs: Seq[Int] =>
      val result = emma.parallelize {
        DataBag(xs).fold2(0) { _ + _ }
      } run runtime

      result should be (xs.sum)
    }
  }

  test("fold3 (sum length)") {
    forAll { xs: Seq[String] =>
      val result = emma.parallelize {
        DataBag(xs).fold3(0)(_.length, _ + _)
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

  test("count, size and length") {
    forAll { xs: Seq[Int] =>
      val (count, size, length) = emma.parallelize {
        val bag = DataBag(xs)
        (bag.count(), bag.size(), bag.length())
      } run runtime

      count  should be (xs count { _ => true })
      size   should be (xs.size)
      length should be (xs.length)
    }
  }

  test("min and max of empty DataBag") {
    intercept[NoSuchElementException] {
      emma.parallelize {
        DataBag(Seq.empty[Int]).min()
      } run runtime
    }

    intercept[NoSuchElementException] {
      emma.parallelize {
        DataBag(Seq.empty[Int]).max()
      } run runtime
    }
  }

  test("min and max") {
    forAll { xs: Seq[Int] =>
      whenever(xs.nonEmpty) {
        val (min, max) = emma.parallelize {
          val bag = DataBag(xs)
          (bag.min(), bag.max())
        } run runtime

        min should be (xs.min)
        max should be (xs.max)
      }
    }
  }

  test("minBy and maxBy (first 5 letters)") {
    forAll { xs: Seq[String] =>
      val (min, max) = emma.parallelize {
        val lt = (x: String, y: String) =>
          x.take(5) < y.take(5)

        val bag = DataBag(xs)
        (bag minBy lt, bag maxBy lt)
      } run runtime

      if (xs.isEmpty) {
        min should be ('empty)
        max should be ('empty)
      } else {
        min.get take 5 should be(xs.map { _ take 5 }.min)
        max.get take 5 should be(xs.map { _ take 5 }.max)
      }
    }
  }

  test("minWith and maxWith (length)") {
    forAll { xs: Seq[String] =>
      val (min, max) = emma.parallelize {
        val bag = DataBag(xs)
        (bag minWith { _.length }, bag maxWith { _.length })
      } run runtime

      if (xs.isEmpty) {
        min should be ('empty)
        max should be ('empty)
      } else {
        min.get.length should be(xs.map { _.length }.min)
        max.get.length should be(xs.map { _.length }.max)
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
        (bag.sum(), bag.product())
      } run runtime

      sum     should be (xs.sum)
      product should be (xs.product)
    }
  }

  test("sumWith and productWith (length)") {
    forAll { xs: Seq[String] =>
      val (sum, product) = emma.parallelize {
        val bag = DataBag(xs)
        (bag sumWith { _.length }, bag productWith { _.length })
      } run runtime

      sum     should be (xs.map { _.length }.sum)
      product should be (xs.map { _.length }.product)
    }
  }

  test("countWith (even)") {
    forAll { xs: Seq[Int] =>
      val even = emma.parallelize {
        DataBag(xs) countWith { _ % 2 == 0 }
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

  test("fold group fusion (char count)") {
    forAll { str: String =>
      val buffer = new ByteArrayOutputStream()
      Console.withOut(buffer) {
        emma.comprehend {
          for (g <- DataBag(str) groupBy identity)
            yield g.key -> g.values.count()
        }
      }

      buffer.toString.toLowerCase should include ("foldgroup")
    }
  }
}
