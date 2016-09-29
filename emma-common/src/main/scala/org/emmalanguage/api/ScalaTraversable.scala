package org.emmalanguage
package api

import scala.language.{higherKinds, implicitConversions}

/** A `DataBag` implementation backed by a Scala `Traversable`. */
class ScalaTraversable[A] private[api](private val rep: Traversable[A]) extends DataBag[A] {

  import ScalaTraversable.wrap

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, p: (B, B) => B): B =
    rep.foldLeft(z)((acc, x) => p(s(x), acc))

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): ScalaTraversable[B] =
    rep.map(f)


  override def flatMap[B: Meta](f: (A) => DataBag[B]): ScalaTraversable[B] =
    rep.flatMap(x => f(x).fetch())

  def withFilter(p: (A) => Boolean): ScalaTraversable[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): ScalaTraversable[Group[K, DataBag[A]]] =
    rep.groupBy(k).toSeq.map { case (key, vals) => Group(key, wrap(vals)) }

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): ScalaTraversable[A] = that match {
    case rdd: ScalaTraversable[A] => this.rep ++ rdd.rep
  }

  override def distinct: ScalaTraversable[A] =
    rep.toSeq.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  override def fetch(): Traversable[A] =
    rep

  // -----------------------------------------------------
  // equals, hashCode and toString
  // -----------------------------------------------------

  override def equals(o: Any) = o match {
    case that: ScalaTraversable[A] =>
      lazy val sizeEq = this.rep.size == that.rep.size
      lazy val diffEm = (this.rep.toSeq diff this.rep.toSeq).isEmpty
      sizeEq && diffEm
    case _ =>
      false
  }

  override def hashCode(): Int =
    scala.util.hashing.MurmurHash3.unorderedHash(rep)

  override def toString: String =
    rep.toString
}

object ScalaTraversable {

  private implicit def wrap[A](rep: Traversable[A]): ScalaTraversable[A] =
    new ScalaTraversable(rep)

  def apply[A: Meta]: ScalaTraversable[A] =
    new ScalaTraversable(Seq.empty)

  def apply[A: Meta](values: Traversable[A]): ScalaTraversable[A] =
    new ScalaTraversable(values)
}