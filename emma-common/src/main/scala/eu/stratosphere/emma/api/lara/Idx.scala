package eu.stratosphere.emma.api.linalg.ir

import spire.math._

import scala.reflect.ClassTag

/**
  * Representation for values with an index.
  *
  * @tparam I the index type
  * @tparam V the value type
  */
// TODO: Solve problem with ordering
//trait Idx[I <: Idx[I, _], V] extends Ordered[I] {
trait Idx[I, V] {
  def id: I

  def value: V

  def map[B](f: (V) => B): Idx[I, B]

  override
  def toString: String = s"($id, $value)"
}

object VIdx {

  private[ir] class IntIdx[V](i: Int, v: V) extends Idx[Int, V] {
    override def id = i

    override def value = v

    //    override def compare(that: Int): Int = this.id - that

    override def map[B](f: (V) => B): Idx[Int, B] = VIdx(id, f(value))
  }

  def apply[V](idx: Int, v: V) = new IntIdx[V](idx, v)

  def apply[V : Numeric : ClassTag](idx: V => Int, v: V) = new IntIdx[V](idx(v), v)

  def project[V, N](idx: V => Int, vle: V => N, v: V) = new IntIdx[N](idx(v), vle(v))
}

object MIdx {

  class IntxIntIdx[V](i: (Int, Int), v: V) extends Idx[(Int, Int), V] {
    override def id = i

    override def value = v

    /** Defines a row major ordering */
    //    override def compare(that: (Int, Int)): Int = {
    //      val res = this.id._1 - that._1
    //      if (res == 0) {
    //        this.id._2 - that._2
    //      } else {
    //        res
    //      }
    //    }

    override def map[B](f: (V) => B): Idx[(Int, Int), B] = MIdx(id, f(value))
  }

  def apply[V](idx: (Int, Int), v: V) = new IntxIntIdx[V](idx, v)

  def apply[V](idx: V => (Int, Int), v: V) = new IntxIntIdx[V](idx(v), v)

  def project[V, N](idx: V => (Int, Int), vle: V => N, v: V) = new IntxIntIdx[N](idx(v), vle(v))
}