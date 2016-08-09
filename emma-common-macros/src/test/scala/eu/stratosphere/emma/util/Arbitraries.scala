package eu.stratosphere.emma.util

import shapeless._
import shapeless.labelled._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

/** Missing instances of [[org.scalacheck.Arbitrary]]. */
trait Arbitraries {

  /** Arbitrary empty product. */
  implicit val hNilArb: Arbitrary[HNil] =
    Arbitrary(Gen.const(HNil))

  /** Arbitrary generic products. */
  implicit def hConsArb[H, T <: HList]
    (implicit H: Arbitrary[H], T: Arbitrary[T]): Arbitrary[H :: T]
    = Arbitrary(for { h <- H.arbitrary; t <- T.arbitrary } yield h :: t)

  /** Arbitrary labelled fields. */
  implicit def fieldArb[K, V](implicit V: Arbitrary[V]): Arbitrary[FieldType[K, V]] =
    Arbitrary(V.arbitrary.map(field[K](_)))
}

/** Missing instances of [[org.scalacheck.Arbitrary]]. */
object Arbitraries extends Arbitraries
