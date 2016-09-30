package eu.stratosphere.emma
package util

import shapeless._

/** Type-level `isEmpty` for [[shapeless.HList]]. */
trait IsEmpty[L <: HList] {
  def apply(): Boolean
}

object IsEmpty {

  /** [[shapeless.HNil]] is empty. */
  implicit val hNil: IsEmpty[HNil] = new IsEmpty[HNil] {
    override def apply(): Boolean = true
  }

  /** [[shapeless.::]] (H-Cons) is non-empty. */
  implicit def hCons[H, T <: HList]: IsEmpty[H :: T] = new IsEmpty[H :: T] {
    override def apply(): Boolean = false
  }
}
