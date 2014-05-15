package eu.stratosphere.emma.macros.algebra

import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context
import eu.stratosphere.emma._

class AlgebraMacros(val c: Context) {

  import c.universe._

  // monad
  def map[T, U](f: c.Expr[T => U])(in: c.Expr[DataBag[T]]): c.Expr[DataBag[U]] = {
    val tree = q"$in.map($f)"
    c.Expr[DataBag[U]](tree)
  }

  def flatMap[T, U](f: c.Expr[T => DataBag[U]])(in: c.Expr[DataBag[U]]): c.Expr[DataBag[U]] = {
    val tree = q"$in.flatMap($f)"
    c.Expr[DataBag[U]](tree)
  }

  def filter[T](p: c.Expr[T => Boolean])(in: c.Expr[DataBag[T]]): c.Expr[DataBag[T]] = {
    val tree = q"$in.withFilter($p)"
    c.Expr[DataBag[T]](tree)
  }

  // cross
  def cross2[T1, T2](in1: c.Expr[DataBag[T1]], in2: c.Expr[DataBag[T1]]) = {
    val tree = q"for (i1 <- $in1; i2 <- $in2) yield (i1, i2)"
    c.Expr[DataBag[(T1, T2)]](tree)
  }

  def cross3[T1, T2, T3](in1: c.Expr[DataBag[T1]], in2: c.Expr[DataBag[T1]], in3: c.Expr[DataBag[T2]]) = {
    val tree = q"for (i1 <- $in1; i2 <- $in2; i3 <- $in3) yield (i1, i2, i3)"
    c.Expr[DataBag[(T1, T2, T3)]](tree)
  }

  def cross4[T1, T2, T3, T4](in1: c.Expr[DataBag[T1]], in2: c.Expr[DataBag[T1]], in3: c.Expr[DataBag[T2]], in4: c.Expr[DataBag[T4]]) = {
    val tree = q"for (i1 <- $in1; i2 <- $in2; i3 <- $in3; i4 <- $in4) yield (i1, i2, i3, i4)"
    c.Expr[DataBag[(T1, T2, T3, T4)]](tree)
  }

  def cross5[T1, T2, T3, T4, T5](in1: c.Expr[DataBag[T1]], in2: c.Expr[DataBag[T2]], in3: c.Expr[DataBag[T3]], in4: c.Expr[DataBag[T4]], in5: c.Expr[DataBag[T5]]) = {
    val tree = q"for (i1 <- $in1; i2 <- $in2; i3 <- $in3; i4 <- $in4; i5 <- $in5) yield (i1, i2, i3, i4, i5)"
    c.Expr[DataBag[(T1, T2, T3, T4, T5)]](tree)
  }

  // join: for (i1 <- in1; i2 <- in2; if k1(i1) == k2(i2)) yield (i1, i2)
  def join[T1: c.WeakTypeTag, T2:  c.WeakTypeTag, K:  c.WeakTypeTag](k1: c.Expr[T1 => K], k2: c.Expr[T2 => K])(in1: c.Expr[DataBag[T1]], in2: c.Expr[DataBag[T2]]): c.Expr[DataBag[(T1, T2)]] = {
    val lhs = replaceVparams(k1.tree.asInstanceOf[Function], List[ValDef](q"val i1: T1".asInstanceOf[ValDef])).body
    val rhs = replaceVparams(k2.tree.asInstanceOf[Function], List[ValDef](q"val i2: T2".asInstanceOf[ValDef])).body
    val tree = q"for (i1 <- $in1; i2 <- $in2; if $lhs == $rhs) yield (i1, i2)"
    c.Expr[DataBag[(T1, T2)]](tree)
  }

  // grouping: for (x <- in) yield for (i <- in; if k(i) == k(x)) yield i
  def groupBy[T, K](k: c.Expr[T => K])(in: c.Expr[DataBag[T]]): c.Expr[DataSet[DataBag[T]]] = {
    val lhs = replaceVparams(k.tree.asInstanceOf[Function], List[ValDef](q"val i: T".asInstanceOf[ValDef])).body
    val rhs = replaceVparams(k.tree.asInstanceOf[Function], List[ValDef](q"val x: T".asInstanceOf[ValDef])).body
    val tree = q"(for (x <- $in) yield for (i <- $in; if $lhs == $rhs) yield i).toSet()"
    c.Expr[DataSet[DataBag[T]]](tree)
  }

  def replaceVparams(fn: Function, vparams: List[ValDef]): Function = {
    val transformer = new VparamsRelacer(fn.vparams zip vparams)
    transformer.transform(fn).asInstanceOf[Function]
  }

  // ---------------------------------------------------
  // Code traversers.
  // ---------------------------------------------------

  private class VparamsRelacer(valdefs: List[(ValDef, ValDef)]) extends Transformer {

    val defsmap = Map() ++ { for (v <- valdefs) yield
      (v._1.name , v)
    }

    override def transform(tree: Tree): Tree = tree match {
      case ident@Ident(name: TermName) =>
        if (defsmap.contains(name))
          Ident(defsmap(name)._2.name)
        else
          ident
      case _ =>
        super.transform(tree)
    }
  }
}
