package eu.stratosphere
package emma.util

import cats.implicits._
import cats.kernel.laws.GroupLaws
import shapeless._
import shapeless.syntax.singleton._

import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers

@RunWith(classOf[JUnitRunner])
class MonoidSpec extends FreeSpec with Checkers with ShapelessEq with ShapelessArb {

  import Monoids._

  def arb[A](implicit Arb: Arbitrary[A]): Gen[A] =
    Arb.arbitrary

  "Monoid laws" - {

    "forgetful" - {

      "with left bias" in {
        check(GroupLaws[Int].monoid(left(0)).all)
      }

      "with right bias" in {
        check(GroupLaws[String].monoid(right("")).all)
      }
    }

    "for maps" - {

      "with overwriting" in {
        check(GroupLaws[Map[Char, Int]].monoid(overwrite).all)
      }

      "with merging" in {
        check(GroupLaws[Map[Char, Int]].monoid(merge).all)
      }
    }

    "for generic products" in {
      check(GroupLaws[HNil].monoid.all)
      check(GroupLaws[Int :: String :: HNil].monoid.all)
    }

    "for generic fields" in {
      val key = "key".witness
      check(GroupLaws[key.T ->> Int].monoid.all)
      check(GroupLaws[key.T ->> String].monoid.all)
    }

    "for sliding collections" in {
      val n = 10
      implicit val vecOfN = Arbitrary(Gen.containerOfN[Vector, Int](n, arb[Int]))
      implicit val setOfN = Arbitrary(Gen.containerOfN[Set, String](n, arb[String]))
      check(GroupLaws[Vector[Int]].monoid(sliding(n)).all)
      check(GroupLaws[Set[String]].monoid(sliding(n)).all)
    }
  }
}
