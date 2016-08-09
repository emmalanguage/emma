package eu.stratosphere
package emma.util

import cats.Eq
import shapeless._

import scala.collection.SortedSet

/** Missing instances of [[cats.Eq]]. */
trait Equivalences {

  /** Equivalence of the empty product. */
  implicit val hNilEq: Eq[HNil] = new Eq[HNil] {
    override def eqv(x: HNil, y: HNil): Boolean = true
  }

  /** Equivalence of generic products. */
  implicit def hConsEq[H, T <: HList](implicit H: Eq[H], T: Eq[T]): Eq[H :: T] = new Eq[H :: T] {
    override def eqv(x: H :: T, y: H :: T): Boolean =
      H.eqv(x.head, y.head) && T.eqv(x.tail, y.tail)
  }

  /** Equivalence of labelled fields. */
  implicit def fieldEq[K, V](implicit V: Eq[V]): Eq[K ->> V] = new Eq[K ->> V] {
    override def eqv(x: K ->> V, y: K ->> V): Boolean = V.eqv(x, y)
  }

  /** Equivalence of sorted sets. */
  implicit def sortedSetEq[A](implicit A: Eq[A]): Eq[SortedSet[A]] = new Eq[SortedSet[A]] {
    override def eqv(x: SortedSet[A], y: SortedSet[A]): Boolean =
      x.size == y.size && x.zip(y).forall((A.eqv _).tupled)
  }
}

/** Missing instances of [[cats.Eq]]. */
object Equivalences extends Equivalences
