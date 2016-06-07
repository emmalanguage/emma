package eu.stratosphere.emma.util

import shapeless._

import org.scalacheck.Arbitrary
import org.scalacheck.Gen

trait HListArb {

  implicit val hnilArb: Arbitrary[HNil] =
    Arbitrary(Gen.const(HNil))

  implicit def hconsArb[H, T <: HList]
    (implicit HArb: Arbitrary[H], TArb: Arbitrary[T]): Arbitrary[H :: T] = {

    Arbitrary(for {
      h <- HArb.arbitrary
      t <- TArb.arbitrary
    } yield h :: t)
  }
}

object HListArb extends HListArb
