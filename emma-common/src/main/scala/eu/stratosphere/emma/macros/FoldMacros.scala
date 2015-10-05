package eu.stratosphere.emma.macros

import eu.stratosphere.emma.api.DataBag

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
 * Macro-based implementations of the methods in the `Folds` trait. These sit on top of the core
 * [[DataBag]] API and depend solely on the [[DataBag#fold]] method. Separating them out like this
 * makes for better extensibility and testability.
 * @param c the current macro context
 */
class FoldMacros(val c: blackbox.Context) extends BlackBoxUtil {
  import universe._

  private lazy val self = unbox(c.prefix.tree)

  // Unbox implicit type conversions
  private def unbox(tree: Tree): Tree =
    tree.unAscribed match {
      case q"$_[$_]($arg)" => arg
      case _               => tree
    }

  def isEmpty = q"!$self.nonEmpty"
  def nonEmpty = q"$self.exists(_ => ${true})"

  def reduce[E](z: Expr[E])(p: Expr[(E, E) => E]) =
    q"$self.fold($z)(_root_.scala.Predef.identity, $p)"

  def reduceOption[E: c.WeakTypeTag](p: Expr[(E, E) => E]) = {
    val x  = freshName("x")
    val y  = freshName("y")
    val oa = weakTypeOf[Option[E]]
    q"""$self.fold($None: $oa)(x => _root_.scala.Some(x): $oa, {
      case ($x, $None) => $x
      case ($None, $y) => $y
      case ($x,    $y) => _root_.scala.Some($p($x.get, $y.get))
    })"""
  }

  def minBy[E: c.WeakTypeTag](p: Expr[(E, E) => Boolean]) = {
    val x = freshName("x")
    val y = freshName("y")
    q"""$self.reduceOption[${weakTypeOf[E]}]({
      case ($x, $y) => if ($p($x, $y)) $x else $y
    })"""
  }

  def maxBy[E](p: Expr[(E, E) => Boolean]) =
    q"$self.minBy(!$p(_, _))"

  def minWith[E, R](f: Expr[E => R])(o: Expr[Ordering[R]]) = {
    val x = freshName("x")
    val y = freshName("y")
    q"$self.minBy({ case ($x, $y) => $o.lt($f($x), $f($y)) })"
  }

  def maxWith[E, R](f: Expr[E => R])(o: Expr[Ordering[R]]) =
    q"$self.minWith($f)($o.reverse)"

  def min[E]()(o: c.Expr[Ordering[E]]) =
    q"$self.reduceOption($o.min).get"

  def max[E]()(o: c.Expr[Ordering[E]]) =
    q"$self.reduceOption($o.max).get"

  def sumWith[E, R](f: Expr[E => R])(n: Expr[Numeric[R]]) =
    q"$self.fold($n.zero)($f, $n.plus)"

  def productWith[A, B](f: Expr[A => B])(n: Expr[Numeric[B]]) =
    q"$self.fold($n.one)($f, $n.times)"

  def sum[E]()(n: Expr[Numeric[E]]) =
    q"$self.reduce($n.zero)($n.plus)"

  def product[E]()(n: Expr[Numeric[E]]) =
    q"$self.reduce($n.one)($n.times)"

  def count() = q"$self.fold(0l)(_ => 1l, _ + _): Long"

  def countWith[E](p: Expr[E => Boolean]) = {
    val x = freshName("x")
    q"""$self.fold(${0l})({
      case $x if $p($x) => ${1l}
      case _            => ${0l}
    }, _ + _)"""
  }

  def exists[E](p: Expr[E => Boolean]) =
    q"$self.fold(${false})($p, _ || _)"

  def forall[E](p: Expr[E => Boolean]) =
    q"$self.fold(${true})($p, _ && _)"

  def find[E: c.WeakTypeTag](p: Expr[E => Boolean]) = {
    val x = freshName("x")
    q"""$self.fold($None: ${weakTypeOf[Option[E]]})({
      case $x if $p($x) => _root_.scala.Some($x)
      case _ => $None
    }, {
      case ($x @ _root_.scala.Some(_), _) => $x
      case (_, $x @ _root_.scala.Some(_)) => $x
      case (_, _) => $None
    })"""
  }

  def bottom[E: c.WeakTypeTag](n: Expr[Int])(o: Expr[Ordering[E]]) =
    q"""$self.fold[${weakTypeOf[List[E]]}]($Nil)(_ :: $Nil,
      _root_.eu.stratosphere.emma.macros.FoldMacros.merge($n, _, _)($o))"""

  def top[A](n: Expr[Int])(o: Expr[Ordering[A]]) =
    q"$self.bottom($n)($o.reverse)"
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
