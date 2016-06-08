package eu.stratosphere
package emma.util

import cats.Monoid
import cats.implicits._
import shapeless._

/** Missing instances of [[cats.Monoid]]. */
object Monoids {

  /** [[shapeless.HNil]] is an empty monoid. */
  implicit val hnil: Monoid[HNil] = new Monoid[HNil] {
    override def empty: HNil = HNil
    override def combine(x: HNil, y: HNil): HNil = HNil
  }

  /** [[shapeless.HList]] is a generic product. */
  implicit def hcons[H, T <: HList](implicit Mh: Monoid[H], Mt: Monoid[T]): Monoid[H :: T] =
    new Monoid[H :: T] {
      override lazy val empty: H :: T = Mh.empty :: Mt.empty
      override def combine(x: H :: T, y: H :: T): H :: T =
        Mh.combine(x.head, y.head) :: Mt.combine(x.tail, y.tail)
    }

  /** Trivial monoid with bias to the left. */
  def left[A](zero: => A): Monoid[A] = new Monoid[A] {
    override lazy val empty: A = zero
    override def combine(x: A, y: A): A = if (x == empty) y else x
  }

  /** Trivial monoid with bias to the right. */
  def right[A](zero: => A): Monoid[A] = new Monoid[A] {
    override lazy val empty: A = zero
    override def combine(x: A, y: A): A = if (y == empty) x else y
  }

  /** Monoid for [[Map]]s that overwrites previous values. */
  implicit def overwrite[K, V]: Monoid[Map[K, V]] = new Monoid[Map[K, V]] {
    override def empty: Map[K, V] = Map.empty
    override def combine(x: Map[K, V], y: Map[K, V]): Map[K, V] = x ++ y
  }

  /** Monoid for [[Map]]s that merges previous values via an underlying monoid. */
  def merge[K, V](implicit M: Monoid[V]): Monoid[Map[K, V]] = new Monoid[Map[K, V]] {
    private def augment(map: Map[K, V]) = map.withDefaultValue(M.empty)
    override lazy val empty: Map[K, V] = augment(Map.empty)
    override def combine(x: Map[K, V], y: Map[K, V]): Map[K, V] =
      y.foldLeft(augment(x)) { case (map, (k, v)) => map + (k -> (map(k) |+| v)) }
  }
}
