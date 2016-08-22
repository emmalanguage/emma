package eu.stratosphere.emma
package compiler

import eu.stratosphere.emma.api.DataBag

import scala.language.higherKinds

/** Dummy IR nodes to be used in the Emma IR. */
package object ir {

  // -----------------------------------------------------------------------
  // Mock comprehension syntax
  // -----------------------------------------------------------------------

  def flatten[A, M[_]](in: M[M[A]]): M[A] = ???

  def generator[A, M[_]](in: M[A]): A = ???

  def comprehension[A, M[_]](block: A): M[A] = ???

  def guard(expr: Boolean): Nothing = ???

  def head[A](expr: A): A = ???

  // -----------------------------------------------------------------------
  // Backend-only combinators
  // -----------------------------------------------------------------------

  def cross[A, B](xs: DataBag[A], ys: DataBag[B]): DataBag[(A,B)] = ???

  def equiJoin[A, B, K](keyx: A => K, keyy: B => K)(xs: DataBag[A], ys: DataBag[B]): DataBag[(A,B)] = ???
}
