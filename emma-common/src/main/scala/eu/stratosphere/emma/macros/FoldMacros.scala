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
package eu.stratosphere
package emma.macros

import emma.api.DataBag

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
 * Macro-based implementations of the methods in the `Folds` trait. These sit on top of the core
 * [[DataBag]] API and depend solely on the [[DataBag#fold]] method. Separating them out like this
 * makes for better extensibility and testability.
 *
 * @param c the current macro context
 */
class FoldMacros(val c: blackbox.Context) extends BlackBoxUtil {

  import universe._
  import syntax._

  private lazy val id = q"_root_.scala.Predef.identity"
  private lazy val option = q"_root_.scala.Option"
  private lazy val some = q"_root_.scala.Some"
  private lazy val list = q"_root_.scala.List"
  private lazy val prefix = c.prefix.tree
  private lazy val self = unbox(prefix)

  // Unbox implicit type conversions
  private def unbox(tree: Tree) =
    unAscribed(tree) match {
      case q"$_[$_]($arg)" => arg
      case _ => c.abort(c.enclosingPosition,
        "Manually instantiating DataBag extensions is not supported")
    }

  def isEmpty = q"!$prefix.nonEmpty"
  def nonEmpty = q"$prefix.exists(_ => ${true})"

  def reduce[E](z: Expr[E])(p: Expr[(E, E) => E]) =
    q"$self.fold($z)($id, $p)"

  def reduceOption[E: c.WeakTypeTag](p: Expr[(E, E) => E]) = {
    val $(x, y, xo, yo) = $("x", "y", "x$opt", "y$opt")
    val E = weakTypeOf[E]
    q"""$self.fold($option.empty[$E])($option(_), ($xo: Option[$E], $yo: Option[$E]) =>
      (for ($x <- $xo; $y <- $yo) yield $p($x, $y)) orElse $xo orElse $yo)"""
  }

  def min[E](o: Expr[Ordering[E]]) =
    q"$prefix.reduceOption($o.min(_, _)).get"

  def max[E](o: Expr[Ordering[E]]) =
    q"$prefix.reduceOption($o.max(_, _)).get"

  def sum[E](n: Expr[Numeric[E]]) =
    q"$prefix.reduce($n.zero)($n.plus)"

  def product[E](n: Expr[Numeric[E]]) =
    q"$prefix.reduce($n.one)($n.times)"

  def size = q"$self.fold(${0L})(_ => ${1L}, _ + _): $LONG"

  def count[E](p: Expr[E => Boolean]) =
    q"$self.fold(${0L})($p(_) compare ${false}, _ + _)"

  def exists[E](p: Expr[E => Boolean]) =
    q"$self.fold(${false})($p, _ || _)"

  def forall[E](p: Expr[E => Boolean]) =
    q"$self.fold(${true})($p, _ && _)"

  def find[E: c.WeakTypeTag](p: Expr[E => Boolean]) =
    q"$self.fold($option.empty[${weakTypeOf[E]}])($some(_) filter $p, _ orElse _)"

  def bottom[E: c.WeakTypeTag](n: Expr[Int])(o: Expr[Ordering[E]]) = {
    val E = weakTypeOf[E]
    val empty = q"$list.empty[$E]"
    q"""if ($n <= ${0}) $empty else $self.fold($empty)(_ :: $Nil,
      _root_.eu.stratosphere.emma.macros.FoldMacros.merge($n, _, _)($o))"""
  }

  def top[E](n: Expr[Int])(o: Expr[Ordering[E]]) =
    q"$prefix.bottom($n)($o.reverse)"

  def sample[E: c.WeakTypeTag](n: Expr[Int]) = {
    val E = weakTypeOf[E]
    val $(x, y, sx, sy, rand, seed) = $("x", "y", "sx", "sy", "rand", "seed")
    val empty = q"$list.empty[$E]"
    val now = q"_root_.java.lang.System.currentTimeMillis"
    q"""if ($n <= ${0}) $empty else $self.fold(($empty, $now))(
      ($x: $E) => ($x :: $Nil, $x.hashCode), {
        case (($x, $sx), ($y, $sy)) =>
          if ($x.size + $y.size <= $n) ($x ::: $y, $sx ^ $sy) else {
            val $seed = $now ^ (($sx << ${Integer.SIZE}) | $sy)
            val $rand = new _root_.scala.util.Random($seed)
            ($rand.shuffle($x ::: $y).take($n), $rand.nextLong())
          }
      })._1"""
  }

  def writeCsv[E: c.WeakTypeTag](location: Expr[String]) = {
    val E = weakTypeOf[E]
    q"""_root_.eu.stratosphere.emma.api.write($location,
      new _root_.eu.stratosphere.emma.api.CSVOutputFormat[$E])($self)"""
  }
}

object FoldMacros {
  import Ordering.Implicits._

  @tailrec def merge[A: Ordering](n: Int, l1: List[A], l2: List[A], acc: List[A] = Nil): List[A] =
    if (acc.length == n) acc.reverse else (l1, l2) match {
      case (x :: t1, y :: _) if x <= y => merge(n, t1, l2, x :: acc)
      case (_, y :: t2) => merge(n, l1, t2, y :: acc)
      case (_, Nil) => acc.reverse ::: l1.take(n - acc.length)
      case (Nil, _) => acc.reverse ::: l2.take(n - acc.length)
      case _ => acc.reverse
    }
}
