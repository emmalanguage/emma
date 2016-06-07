package eu.stratosphere.emma.util

import shapeless._
import shapeless.labelled._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

/** Instances of [[Arbitrary]] for [[shapeless]]. */
trait ShapelessArb {

  implicit val hnilArb: Arbitrary[HNil] =
    Arbitrary(Gen.const(HNil))

  implicit def hconsArb[H, T <: HList]
    (implicit H: Arbitrary[H], T: Arbitrary[T]): Arbitrary[H :: T] = {

    Arbitrary(for {
      h <- H.arbitrary
      t <- T.arbitrary
    } yield h :: t)
  }

  implicit def kvArb[K, V](implicit V: Arbitrary[V]): Arbitrary[FieldType[K, V]] =
    Arbitrary(V.arbitrary.map(field[K](_)))
}

/** Instances of [[Arbitrary]] for [[shapeless]]. */
object ShapelessArb extends ShapelessArb
