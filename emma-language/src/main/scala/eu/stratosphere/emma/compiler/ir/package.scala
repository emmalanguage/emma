package eu.stratosphere.emma
package compiler

import scala.language.higherKinds

/** Dummy IR nodes to be used in the Emma IR. */
package object ir {

  def flatten[A, M[_]](in: M[M[A]]): M[A] = ???

  def generator[A, M[_]](in: M[A]): A = ???

  def comprehension[A, M[_]](block: A): M[A] = ???

  def guard(expr: Boolean): Nothing = ???

  def head[A](expr: A): A = ???
}
