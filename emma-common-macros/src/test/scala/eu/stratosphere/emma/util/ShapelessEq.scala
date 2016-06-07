package eu.stratosphere
package emma.util

import cats.Eq
import shapeless._

/** Instances of [[Eq]] for [[shapeless]]. */
trait ShapelessEq {

  implicit val hnilEq: Eq[HNil] = new Eq[HNil] {
    override def eqv(x: HNil, y: HNil): Boolean = true
  }

  implicit def hconsEq[H, T <: HList](implicit H: Eq[H], T: Eq[T]): Eq[H :: T] =
    new Eq[H :: T] {
      override def eqv(x: H :: T, y: H :: T): Boolean =
        H.eqv(x.head, y.head) && T.eqv(x.tail, y.tail)
    }

  implicit def kvEq[K, V](implicit V: Eq[V]): Eq[K ->> V] = new Eq[K ->> V] {
    override def eqv(x: K ->> V, y: K ->> V): Boolean = V.eqv(x, y)
  }
}

/** Instances of [[Eq]] for [[shapeless]]. */
object ShapelessEq extends ShapelessEq
