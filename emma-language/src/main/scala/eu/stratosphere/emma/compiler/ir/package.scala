package eu.stratosphere.emma
package compiler

import eu.stratosphere.emma.api.DataBag

/** Dummy IR nodes to be used in the Emma IR. */
package object ir {

  def flatten[A](in: DataBag[DataBag[A]]): DataBag[A] = ???

  def generator[A](in: DataBag[A]): A = ???

  def comprehension[A](block: A) = ???

  def guard(expr: Boolean) = ???
}
