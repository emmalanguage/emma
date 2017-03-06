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
package api.backend

import scala.Function.const
import scala.annotation.tailrec
import scala.util.Random

/**
 * Backend representation nodes for expanded `DataBag` folds.
 * These provide implementations of the `emp`, `sng` and `uni` functions
 * corresponding to folds available in the `DataBag` API.
 */
object Aggregations {

  /** Lifts a binary function to work in the [[Option]] alternative. */
  private def liftOpt[A](op: (A, A) => A): (Option[A], Option[A]) => Option[A] = {
    case (Some(x), Some(y)) => Option(op(x, y))
    case (xo, yo) => xo orElse yo
  }

  /** An aggregation represents `DataBag[A].fold[B](emp)(sng, uni)`. */
  sealed trait Agg[-A, B] extends Serializable {
    def emp: B
    def sng: A => B
    def uni: (B, B) => B
  }

  /** `xs.fold(emp)(sng, uni)` */
  case class Fold[-A, B](emp: B, sng: A => B, uni: (B, B) => B)
    extends Agg[A, B]

  /** `xs.reduce(emp)(uni)` */
  case class Reduce[A](emp: A)(val uni: (A, A) => A) extends Agg[A, A] {
    val sng: A => A = identity
  }

  /** `xs.reduceOption(op)` */
  case class ReduceOpt[A](op: (A, A) => A) extends Agg[A, Option[A]] {
    def emp: Option[A] = None
    val sng: A => Option[A] = Option.apply
    val uni: (Option[A], Option[A]) => Option[A] = liftOpt(op)
  }

  /** `xs.isEmpty` */
  case object IsEmpty extends Agg[Any, Boolean] {
    def emp: Boolean = true
    val sng: Any => Boolean = const(false)
    val uni: (Boolean, Boolean) => Boolean = _ && _
  }

  /** `xs.nonEmpty` */
  case object NonEmpty extends Agg[Any, Boolean] {
    def emp: Boolean = false
    val sng: Any => Boolean = const(true)
    val uni: (Boolean, Boolean) => Boolean = _ || _
  }

  /** `xs.size` */
  case object Size extends Agg[Any, Long] {
    def emp: Long = 0
    val sng: Any => Long = const(1)
    val uni: (Long, Long) => Long = _ + _
  }

  /** `xs.count(p)` */
  case class Count[A](p: A => Boolean) extends Agg[A, Long] {
    def emp: Long = 0
    val sng: A => Long = p(_) compare false
    val uni: (Long, Long) => Long = _ + _
  }

  /** `xs.min(ord)` */
  case class Min[A](ord: Ordering[A]) extends Agg[A, Option[A]] {
    def emp: Option[A] = None
    val sng: A => Option[A] = Option.apply
    val uni: (Option[A], Option[A]) => Option[A] = liftOpt(ord.min)
  }

  /** `xs.max(ord)` */
  case class Max[A](ord: Ordering[A]) extends Agg[A, Option[A]] {
    def emp: Option[A] = None
    val sng: A => Option[A] = Option.apply
    val uni: (Option[A], Option[A]) => Option[A] = liftOpt(ord.max)
  }

  /** `xs.sum(num)` */
  case class Sum[A](num: Numeric[A]) extends Agg[A, A] {
    def emp: A = num.zero
    val sng: A => A = identity
    val uni: (A, A) => A = num.plus
  }

  /** `xs.product(num)` */
  case class Product[A](num: Numeric[A]) extends Agg[A, A] {
    def emp: A = num.one
    val sng: A => A = identity
    val uni: (A, A) => A = num.times
  }

  /** `xs.exists(p)` */
  case class Exists[A](sng: A => Boolean) extends Agg[A, Boolean] {
    def emp: Boolean = false
    val uni: (Boolean, Boolean) => Boolean = _ || _
  }

  /** `xs.forall(p)` */
  case class Forall[A](sng: A => Boolean) extends Agg[A, Boolean] {
    def emp: Boolean = true
    val uni: (Boolean, Boolean) => Boolean = _ && _
  }

  /** `xs.find(p)` */
  case class Find[A](p: A => Boolean) extends Agg[A, Option[A]] {
    def emp: Option[A] = None
    val sng: A => Option[A] = Option(_) filter p
    val uni: (Option[A], Option[A]) => Option[A] = _ orElse _
  }

  /** `xs.bottom(n)(ord)` */
  case class Bottom[A](n: Int, ord: Ordering[A]) extends Agg[A, List[A]] {
    def emp: List[A] = Nil
    val sng: A => List[A] = _ :: Nil
    val uni: (List[A], List[A]) => List[A] = merge(_, _)

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

  /** `xs.top(n)(ord)` */
  case class Top[A](n: Int, ord: Ordering[A]) extends Agg[A, List[A]] {
    private val bottom = Bottom(n, ord.reverse)
    def emp: List[A] = bottom.emp
    def sng: A => List[A] = bottom.sng
    def uni: (List[A], List[A]) => List[A] = bottom.uni
  }

  /** `xs.sample(n)` */
  case class Sample[A](n: Int) extends Agg[A, (Long, List[A])] {
    private def now = System.nanoTime()
    def emp: (Long, List[A]) = (now, Nil)
    val sng: A => (Long, List[A]) = x => (now ^ x.hashCode, x :: Nil)
    val uni: ((Long, List[A]), (Long, List[A])) => (Long, List[A]) = {
      case ((hx, xs), (hy, ys)) =>
        val pair @ (seed, xys) = (hx ^ hy, xs ++ ys)
        if (xys.size <= n) pair else {
          val rand = new Random(seed)
          (rand.nextLong(), rand.shuffle(xys).take(n))
        }
    }
  }
}
