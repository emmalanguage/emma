package eu.stratosphere
package emma.util

import cats.implicits._
import cats.kernel.laws.GroupLaws
import shapeless._

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers

@RunWith(classOf[JUnitRunner])
class MonoidSpec extends FreeSpec with Checkers with HListEq with HListArb {

  import Monoids._

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
  }
}
