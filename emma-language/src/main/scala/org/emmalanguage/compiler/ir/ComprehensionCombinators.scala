package org.emmalanguage
package compiler.ir

import eu.stratosphere.emma.api.DataBag

import scala.language.higherKinds

/** Dummy IR nodes that model comprehension combinators in the Emma IR. */
object ComprehensionCombinators {

  def cross[A, B](xs: DataBag[A], ys: DataBag[B]): DataBag[(A,B)] = ???

  def equiJoin[A, B, K](keyx: A => K, keyy: B => K)(xs: DataBag[A], ys: DataBag[B]): DataBag[(A,B)] = ???

}
