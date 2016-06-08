package eu.stratosphere
package emma.util

import cats.Eq
import shapeless._

trait HListEq {

  implicit val hnilEq: Eq[HNil] = new Eq[HNil] {
    override def eqv(x: HNil, y: HNil): Boolean = true
  }

  implicit def hconsEq[H, T <: HList](implicit HEq: Eq[H], TEq: Eq[T]): Eq[H :: T] =
    new Eq[H :: T] {
      override def eqv(x: H :: T, y: H :: T): Boolean =
        HEq.eqv(x.head, y.head) && TEq.eqv(x.tail, y.tail)
    }
}

object HListEq extends HListEq
