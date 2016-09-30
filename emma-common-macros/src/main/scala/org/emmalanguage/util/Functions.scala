package org.emmalanguage
package util

import shapeless._

/** Utilities for [[scala.Function]]s. */
object Functions {

  /** Applies a partial function to the `args` or else return `default`. */
  def complete[A, R](pf: A =?> R)(args: A)(default: => R): R =
    pf.applyOrElse(args, (_: A) => default)

  /** Forgets the head of the returned [[HList]]. */
  def tail[A, H, T <: HList](f: A => (H :: T)): A => T =
    f.andThen(_.tail)
}
