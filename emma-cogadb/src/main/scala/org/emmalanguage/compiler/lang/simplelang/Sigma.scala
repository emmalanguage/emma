/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package compiler.lang.simplelang

/** An signature sigma models the (abstract) signature for a set of functions that return the same type `A` (sometimes
 * called carrier type or carrier set).
 */
abstract class Sigma[A] {
  //@formatter:off
  def `true`: A
  def `false`: A
  def `0`: A

  def succ(t1: A): A
  def iszero(t1: A): A
  def ifThenElse(t1: A, t2: A, t3: A): A
  //@formatter:on
}

object Sigma {

  /** An algebra is an implementation of the signature for a specific carrier set (for example Unit)
   *
   * Carrier sets are usually not initial. Initiality is volated if:
   *
   * (confusion) there exists an `a` from `A` which be represented as a result of two distinct `Sigma[A]` expressions.
   *
   * In other words, if `Sigma[A]` is not injective. For example the following `Sigma[Unit]` definition
   * identifies both `true` and `false` als `()` - the only element of the singleton type `Unit`.
   */
  object UnitAlgebra extends Sigma[Unit] {
    //@formatter:off
    def `true`: Unit = ()
    def `false`: Unit = ()
    def `0`: Unit = ()

    def succ(t1: Unit): Unit = ()
    def iszero(t1: Unit): Unit = ()
    def ifThenElse(t1: Unit, t2: Unit, t3: Unit): Unit = ()
    //@formatter:on
  }

  /**
   * Initiality is also volated if:
   *
   * (junk) there exists an `a` from `A` which can not be represented as a result of a `Sigma[A]` expression.
   *
   * In other words, if `Sigma[A]` is not surjective. For example the following `Sigma[Int]` definition
   * cannot represent negative integers.
   */
  object IntAlgebra extends Sigma[Int] {
    //@formatter:off
    override def `true`: Int = 1
    override def `false`: Int = 0
    override def `0`: Int = 0

    override def succ(t1: Int): Int = t1 + 1
    override def iszero(t1: Int): Int = if (t1 == 0) `true` else `false`
    override def ifThenElse(t1: Int, t2: Int, t3: Int): Int = if (t1 == `true`) t2 else t3
    //@formatter:on
  }

  /**
   * In other words, the existence of an initial carrier `T` set implies the existence of an inverse to `Sigma[T]`.
   * Due to Lambek's lemma (or Lambek's fixpoint theorem), for polynomial signatures (such as the one above), such
   * a carrier is shown to always exist.
   *
   * Here is how we can define one in Scala (up to isomorphism, meaning that the names are not important).
   */
  //@formatter:off
  trait Term
  case object True extends Term
  case object False extends Term
  case object Zero extends Term

  case class Succ(t1: Term) extends Term
  case class IsZero(t1: Term) extends Term
  case class IfThenElse(t1: Term, t2: Term, t3: Term) extends Term
  //@formatter:on

  /**
   * And here is the corresponding `Sigma[A]` algebra (sometimes called initial algebra or term algebra).
   */
  object TermAlgebra extends Sigma[Term] {
    //@formatter:off
    override def `true`: Term = True
    override def `false`: Term = False
    override def `0`: Term = Zero

    override def succ(t1: Term): Term = Succ(t1)
    override def iszero(t1: Term): Term = IsZero(t1)
    override def ifThenElse(t1: Term, t2: Term, t3: Term): Term = IfThenElse(t1, t2, t3)
    //@formatter:on
  }

  /**
   * Finally, for each initial algebra, we can define a recursion scheme based on the structure (or the shape)
   * of Sigma. The recursion scheme is parameterized by another `Sigma[A]` instance and yields a `Term -> A` function.
   * Such recursion schemes are commonly known as folds.
   */
  def fold[A](a: Sigma[A]): Term => A = {
    case True => a.`true`
    case False => a.`false`
    case Zero => a.`0`

    case Succ(t1) => a.succ(fold(a)(t1))
    case IsZero(t1) => a.iszero(fold(a)(t1))
    case IfThenElse(t1, t2, t3) => a.ifThenElse(fold(a)(t1), fold(a)(t2), fold(a)(t3))
  }

  /**
   * Various functions from PL can be specified as folds, most notably the interpretation function eval
   */
  object Eval extends Sigma[Either[Boolean, Int]] {
    type T = Either[Boolean, Int]

    //@formatter:off
    override def `true`: T = Left(true)
    override def `false`: T = Left(false)
    override def `0`: T = Right(0)

    override def succ(t1: T): T = t1 match {
      case Left(_) => throw new RuntimeException("Unexpected succ argument type Boolean")
      case Right(t) => Right(t + 1)
    }
    override def iszero(t1: T): T = t1 match {
      case Left(_) => throw new RuntimeException("Unexpected iszero argument type Boolean")
      case Right(t) => Left(t == 0)
    }
    override def ifThenElse(t1: T, t2: T, t3: T): T = t1 match {
      case Left(true) => t2
      case Left(false) => t3
      case Right(_) => throw new RuntimeException("Unexpected  condition type Int")
    }
    //@formatter:on
  }

  // operational semantics can be specified as a fold
  val eval = fold(Eval)

  /** Examples. */
  def main(args: Array[String]): Unit = {

    // if true then false else true
    val t1 = IfThenElse(True, False, True)
    val r1 = eval(t1) // Left(false)
    println(r1)

    // if x == 0 then x else x + 1
    val t2 = (x: Term) => IfThenElse(IsZero(x), x, Succ(x))
    val r2 = eval(t2(Succ(Zero))) // Right(2)
    println(r2)
  }
}
