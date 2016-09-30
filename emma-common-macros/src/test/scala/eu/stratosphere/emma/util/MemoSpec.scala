package eu.stratosphere.emma
package util

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class MemoSpec extends FreeSpec with Matchers {

  "Momoization" - {

    "of simple functions" in {
      val string = "the quick brown fox jumps over the lazy dog"
      val cache = mutable.Map.empty[Char, Int]
      var computed = Vector.empty[Char]

      val indexOf = Memo[Char, Int]({ ch =>
        computed :+= ch
        string.indexOf(ch)
      }, cache)

      val result = "this is a string with duplicate characters"
        .map(ch => ch -> indexOf(ch)).toMap

      computed should contain theSameElementsAs cache.keys
      result should contain theSameElementsAs cache
    }

    "of recursive functions" in {
      val levenshtein = Memo.recur[(String, String), Int](levenshtein => {
        case ("", ys) => ys.length
        case (xs, "") => xs.length
        case (xs, ys) =>
          val delta = if (xs.head == ys.head) 0 else 1
          val (xt, yt) = (xs.tail, ys.tail)
          Seq(
            levenshtein(xt, yt) + delta,
            levenshtein(xt, ys) + 1,
            levenshtein(xs, yt) + 1
          ).min
      })

      levenshtein("", "") should be (0)
      levenshtein("", "123456") should be (6)
      levenshtein("xyx", "xyyyx") should be (2)
      levenshtein("kitten", "sitting") should be (3)
      levenshtein("closure", "clojure") should be (1)
      levenshtein("ttttattttctg", "tcaaccctaccat") should be (10)
      levenshtein("gaattctaatctc", "caaacaaaaaattt") should be (9)
    }

    "of commutative functions" in {
      val cache = mutable.Map.empty[(Int, Int), Int]
      var computed = Vector.empty[(Int, Int)]

      val add = Memo.commutative[Int, Int]({ case (x, y) =>
        computed +:= (x, y)
        x + y
      }, cache)

      val result = (for {
        x <- 1 to 10
        y <- 1 to 10
        if x != y
      } yield add(x, y)).sum

      computed should contain theSameElementsAs cache.keys
      result should equal (cache.values.sum * 2)
    }
  }
}
