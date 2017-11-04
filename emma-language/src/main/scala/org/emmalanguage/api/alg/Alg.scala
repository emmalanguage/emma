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
package api.alg

import api.DataBag
import api.Meta

import scala.Function.const
import scala.annotation.tailrec

/**
 * A (non-initial) union-representation algebra.
 *
 * The following identities must hold forall `x`, `y`, `z`:
 *
 * {{{
 *         plus x zero == x == plus zero x
 *   plus x (plus y z) == plus (plus x y) z
 *            plus x y == plus y x
 * }}}
 *
 * Instances of this type model parallel computations.
 */
trait Alg[-A, B] extends Serializable {
  val zero: B
  val init: A => B
  val plus: (B, B) => B
}

object Alg {
  /** Lifts a binary function to work in the [[Option]] alternative. */
  private[alg] def liftOpt[A](op: (A, A) => A): (Option[A], Option[A]) => Option[A] = {
    case (Some(x), Some(y)) => Option(op(x, y))
    case (xo, yo) => xo orElse yo
  }
}

/** Constructed by `xs.fold(z)(i, p)`. */
case class Fold[A, B](z: B, i: A => B, p: (B, B) => B) extends Alg[A, B] {
  val zero: B = z
  val init: A => B = i
  val plus: (B, B) => B = p
}

// -------------------------------------------------------
// Algebras with possibly native implementations
// -------------------------------------------------------

/** Constructed by `xs.reduce(z)(p)`. */
case class Reduce[A](z: A, p: (A, A) => A) extends Alg[A, A] {
  val zero: A = z
  val init: A => A = identity[A]
  val plus: (A, A) => A = p
}

/** Constructed by `xs.reduceOption(p)`. */
case class ReduceOpt[A](p: (A, A) => A) extends Alg[A, Option[A]] {
  val zero: Option[A] = None
  val init: A => Option[A] = Option.apply[A]
  val plus: (Option[A], Option[A]) => Option[A] = Alg.liftOpt(p)
}

/** Constructed by `xs.isEmpty`. */
case object IsEmpty extends Alg[Any, Boolean] {
  val zero: Boolean = true
  val init: Any => Boolean = const(false)
  val plus: (Boolean, Boolean) => Boolean = _ && _
}

/** Constructed by `xs.nonEmpty`. */
case object NonEmpty extends Alg[Any, Boolean] {
  val zero: Boolean = false
  val init: Any => Boolean = const(true)
  val plus: (Boolean, Boolean) => Boolean = _ || _
}

/** Constructed by `xs.size`. */
case object Size extends Alg[Any, Long] {
  val zero: Long = 0
  val init: Any => Long = const(1)
  val plus: (Long, Long) => Long = _ + _
}

/** Constructed by `xs.count(p)`. */
case class Count[A](p: A => Boolean) extends Alg[A, Long] {
  val zero: Long = 0
  val init: A => Long = x => if (p(x)) 1 else 0
  val plus: (Long, Long) => Long = _ + _
}

/** Constructed by `xs.min(ord)`. */
case class Min[A](ord: Ordering[A]) extends Alg[A, Option[A]] {
  val zero: Option[A] = None
  val init: A => Option[A] = Option.apply
  val plus: (Option[A], Option[A]) => Option[A] = Alg.liftOpt(ord.min)
}

/** Constructed by `xs.max(ord)`. */
case class Max[A](ord: Ordering[A]) extends Alg[A, Option[A]] {
  val zero: Option[A] = None
  val init: A => Option[A] = Option.apply
  val plus: (Option[A], Option[A]) => Option[A] = Alg.liftOpt(ord.max)
}

/** Constructed by `xs.sum(num)`. */
case class Sum[A](num: Numeric[A]) extends Alg[A, A] {
  val zero: A = num.zero
  val init: A => A = identity
  val plus: (A, A) => A = num.plus
}

/** Constructed by `xs.product(num)`. */
case class Product[A](num: Numeric[A]) extends Alg[A, A] {
  val zero: A = num.one
  val init: A => A = identity
  val plus: (A, A) => A = num.times
}

/** Constructed by `xs.exists(p)`. */
case class Exists[A](init: A => Boolean) extends Alg[A, Boolean] {
  val zero: Boolean = false
  val plus: (Boolean, Boolean) => Boolean = _ || _
}

/** Constructed by `xs.forall(p)`. */
case class Forall[A](init: A => Boolean) extends Alg[A, Boolean] {
  val zero: Boolean = true
  val plus: (Boolean, Boolean) => Boolean = _ && _
}

/** Constructed by `xs.find(p)`. */
case class Find[A](p: A => Boolean) extends Alg[A, Option[A]] {
  val zero: Option[A] = None
  val init: A => Option[A] = Option(_) filter p
  val plus: (Option[A], Option[A]) => Option[A] = _ orElse _
}

/** Constructed by `xs.bottom(n)(ord)`. */
case class Bottom[A](n: Int, ord: Ordering[A]) extends Alg[A, List[A]] {
  val zero: List[A] = Nil
  val init: A => List[A] = _ :: Nil
  val plus: (List[A], List[A]) => List[A] = merge(_, _)

  @tailrec
  private def merge(xs: List[A], yx: List[A], acc: List[A] = Nil, len: Int = 0): List[A] =
    (xs, yx) match {
      case (Nil, _) => acc reverse_::: yx.take(n - len)
      case (_, Nil) => acc reverse_::: xs.take(n - len)
      case (x :: tx, y :: ty) if len < n =>
        if (ord.lteq(x, y)) merge(tx, yx, x :: acc, len + 1)
        else merge(xs, ty, y :: acc, len + 1)
      case _ => acc.reverse
    }
}

/** Constructed by `xs.top(n)(ord)`. */
case class Top[A](n: Int, ord: Ordering[A]) extends Alg[A, List[A]] {
  private val bottom = Bottom(n, ord.reverse)
  val zero: List[A] = bottom.zero
  val init: A => List[A] = bottom.init
  val plus: (List[A], List[A]) => List[A] = bottom.plus
}

// -------------------------------------------------------
// Algebras encoding fused MonadOps
// -------------------------------------------------------

/** A subtree of folds with inputs converted from type [[A]] to type [[B]]. */
case class Map[A, B, C](f: A => B, child: Alg[B, C]) extends Alg[A, C] {
  val zero: C = child.zero
  val init: A => C = f andThen child.init
  val plus: (C, C) => C = child.plus
}

/** A subtree of folds with inputs conditioned in a predicate [[f]]. */
case class FlatMap[A, B, C: Meta](f: A => DataBag[B], child: Alg[B, C]) extends Alg[A, C] {
  val zero: C = child.zero
  val init: A => C = f andThen (_.fold(child))
  val plus: (C, C) => C = child.plus
}

/** A subtree of folds with inputs conditioned in a predicate [[p]]. */
case class WithFilter[A, B](p: A => Boolean, child: Alg[A, B]) extends Alg[A, B] {
  val zero: B = child.zero
  val init: A => B = (x: A) => if (p(x)) child.init(x) else child.zero
  val plus: (B, B) => B = child.plus
}
