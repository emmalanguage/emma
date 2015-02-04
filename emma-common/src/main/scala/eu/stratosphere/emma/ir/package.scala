package eu.stratosphere.emma

import java.util.UUID

import eu.stratosphere.emma.api.{DataBag, InputFormat, OutputFormat}

import scala.collection.mutable

/**
 * Nodes for building an intermediate representation of an Emma dataflows.
 */
package object ir {

  import scala.language.implicitConversions
  import scala.language.existentials
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox

  // ---------------------------------------------------
  // Thunks.
  // ---------------------------------------------------

  trait ValueRef[A] {

    val name: String

    def uuid: UUID = UUID.nameUUIDFromBytes(name.getBytes)

    def value: A
  }

  implicit def ValueRefToValue[A](ref: ValueRef[A]): A = ref.value

  // --------------------------------------------------------------------------
  // Combinators.
  // --------------------------------------------------------------------------

  sealed trait Combinator[+A] {
    implicit val tag: TypeTag[_ <: A]

    /** Apply `f` to each subtree */
    def sequence() = collect({
      case x => x
    })

    /** Apply `pf` to each subexpression on which the function is defined and collect the results. */
    def collect[T](pf: PartialFunction[Combinator[_], T]): List[T] = {
      val ctt = new CollectTreeTraverser[T](pf)
      ctt.traverse(this)
      ctt.results.toList
    }
  }

  final case class Read[+A: TypeTag](location: String, format: InputFormat[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Write[+A: TypeTag](location: String, format: OutputFormat[_ <: A], xs: Combinator[_ <: A]) extends Combinator[Unit] {
    override val tag: TypeTag[Unit] = typeTag[Unit]
  }

  final case class TempSource[+A: TypeTag](ref: ValueRef[_ <: DataBag[A]]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class TempSink[+A: TypeTag](name: String, xs: Combinator[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class FoldSink[+A: TypeTag](name: String, xs: Fold[_ <: A, Any]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Map[+A: TypeTag, +B: TypeTag](f: Expr[Any], xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class FlatMap[+A: TypeTag, +B: TypeTag](f: Expr[Any], xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Filter[+A: TypeTag](p: Expr[Any], xs: Combinator[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class EquiJoin[+A: TypeTag, +B: TypeTag, +C: TypeTag](keyx: Expr[Any], keyy: Expr[Any], xs: Combinator[_ <: B], ys: Combinator[_ <: C]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
    lazy val f: Expr[Any] = reify((x: B, y: C) => (x, y))
  }

  final case class Cross[+A: TypeTag, +B: TypeTag, +C: TypeTag](xs: Combinator[_ <: B], ys: Combinator[_ <: C]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
    lazy val f: Expr[Any] = reify((x: B, y: C) => (x, y))
  }

  final case class Group[+A: TypeTag, +B: TypeTag](key: Expr[Any], xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Fold[+A: TypeTag, +B: TypeTag](empty: Expr[Any], sng: Expr[Any], union: Expr[Any], xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class FoldGroup[+A: TypeTag, +B: TypeTag](key: Expr[Any], empty: Expr[Any], sng: Expr[Any], union: Expr[Any], xs: Combinator[_ <: B]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Distinct[+A: TypeTag](xs: Combinator[A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Union[+A: TypeTag](xs: Combinator[_ <: A], ys: Combinator[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Diff[+A: TypeTag](xs: Combinator[_ <: A], ys: Combinator[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  final case class Scatter[+A: TypeTag](xs: Seq[_ <: A]) extends Combinator[A] {
    override val tag: TypeTag[_ <: A] = typeTag[A]
  }

  // --------------------------------------------------------------------------
  // Traversal
  // --------------------------------------------------------------------------

  trait CombinatorTraverser {

    def traverse(e: Combinator[_]): Unit = e match {
      // Combinators
      case Read(_, _) =>
      case Write(_, _, xs) => traverse(xs)
      case TempSource(id) =>
      case TempSink(_, xs) => traverse(xs)
      case FoldSink(_, xs) => traverse(xs)
      case Map(_, xs) => traverse(xs)
      case FlatMap(_, xs) => traverse(xs)
      case Filter(_, xs) => traverse(xs)
      case EquiJoin(_, _, xs, ys) => traverse(xs); traverse(ys)
      case Cross(xs, ys) => traverse(xs); traverse(ys)
      case Group(_, xs) => traverse(xs)
      case Fold(_, _, _, xs) => traverse(xs)
      case FoldGroup(_, _, _, _, xs) => traverse(xs)
      case Distinct(xs) => traverse(xs)
      case Union(xs, ys) => traverse(xs); traverse(ys)
      case Diff(xs, ys) => traverse(xs); traverse(ys)
      case Scatter(_) =>
    }
  }

  private class CollectTreeTraverser[T](pf: PartialFunction[Combinator[_], T]) extends CombinatorTraverser {
    val results = mutable.ListBuffer[T]()

    override def traverse(t: Combinator[_]) {
      super.traverse(t)
      if (pf.isDefinedAt(t)) results += pf(t)
    }
  }

  // --------------------------------------------------------------------------
  // Auxiliary structures
  // --------------------------------------------------------------------------

  def localInputs(e: Combinator[_]): Seq[Seq[_]] = e.collect({
    case Scatter(xs) => xs
  })

  final class UDF(fn: Function, tpe: Type, tb: ToolBox[ru.type]) {

    import UDF.fnSymbols

    val tree = tpe match {
      case TypeRef(prefix, sym, targs) if fnSymbols.contains(sym) =>
        // find all "free" symbols in the UDF body
        val freeSymbols = fn.body.collect({
          case i@Ident(TermName(_)) if i.symbol.toString.startsWith("free term") => i.symbol.asTerm
        })
        // compute a typed closure list
        val closure: List[ValDef] = for (s <- freeSymbols) yield ValDef(Modifiers(Flag.PARAM), s.name, tq"${s.info}", EmptyTree)
        // compute a typed params list
        val params: List[ValDef] = for ((param, paramTypes) <- fn.vparams zip targs) yield ValDef(param.mods, param.name, tq"$paramTypes", param.rhs)
        // create a typed curried form of the UDF (closure) => (params) => body
        tb.typecheck(q"(..$closure) => (..$params) => ${fn.body}").asInstanceOf[Function]
      case _ =>
        throw new RuntimeException(s"Unsupported UDF type '$tpe'. Only (params) => (body) with up to 10 params are supported at the moment.")
    }

    def closure = tree.vparams

    def params = tree.body.asInstanceOf[Function].vparams

    def body = tree.body.asInstanceOf[Function].body
  }

  object UDF {

    val fnSymbols = Set[Symbol](
      rootMirror.staticClass("scala.Function0"),
      rootMirror.staticClass("scala.Function1"),
      rootMirror.staticClass("scala.Function2"),
      rootMirror.staticClass("scala.Function3"),
      rootMirror.staticClass("scala.Function4"),
      rootMirror.staticClass("scala.Function5"),
      rootMirror.staticClass("scala.Function6"),
      rootMirror.staticClass("scala.Function7"),
      rootMirror.staticClass("scala.Function8"),
      rootMirror.staticClass("scala.Function9"),
      rootMirror.staticClass("scala.Function10"))

    def apply(fn: Function, tpe: Type, tb: ToolBox[ru.type]) = new UDF(tb.untypecheck(fn).asInstanceOf[Function], tpe, tb)

    def apply(expr: Expr[Any], tb: ToolBox[ru.type]) = new UDF(tb.untypecheck(expr.tree).asInstanceOf[Function], expr.staticType, tb)
  }

}
