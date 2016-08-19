package eu.stratosphere.emma
package compiler.lang.source

import compiler.Common

import cats.std.all._
import shapeless._

import scala.collection.generic.{CanBuildFrom, FilterMonadic}

/** Eliminates `foreach` calls and replaces them with `while` loops. */
private[source] trait Foreach2Loop extends Common {
  self: Source =>

  import Source.{Lang => src}
  import UniverseImplicits._
  import api.TermName.fresh

  /**
   * Converts a `foreach` method call to a `while` loop:
   *
   * {{{
   *   for (x <- xs) println(x)
   *   // equivalently
   *   xs foreach println
   *   // becomes
   *   {
   *     val iter = xs.toIterator
   *     var x = null
   *     while (iter.hasNext) {
   *       x = iter.next()
   *       println(x)
   *     }
   *   }
   * }}}
   */
  private[source] object Foreach2Loop {

    /** The Foreach2Loop transformation. */
    lazy val transform: u.Tree => u.Tree = {
      val asInst = api.Type.any.member(api.TermName("asInstanceOf")).asTerm
      api.BottomUp.withOwner
        // Collect variable definitions
        .synthesize(Attr.collect[Set, u.TermSymbol] {
        case src.VarDef(lhs, _, _) => lhs
      })
        // Collect variable assignments
        .synthesize(Attr.collect[Set, u.TermSymbol] {
        case src.VarMut(lhs, _) => lhs
      }).transformWith {
        case Attr(
        src.DefCall(xs @ Some(_ withType xsTpe), method, _,
        Seq(src.Lambda(_, Seq(src.ParDef(arg, _, _)), body))),
        _, owner :: _, muts :: defs :: _
        ) if !muts.subsetOf(defs)
          && (method == FM.foreach
          || method == api.Sym.foreach
          || method.overrides.contains(api.Sym.foreach)
          ) =>

          val Elem = api.Type.of(arg)
          val elem = api.VarSym(owner, fresh(arg), Elem)
          val (trav, tpe) = if (method == FM.foreach) {
            val Repr = api.Type.arg(2, xsTpe)
            val CBF = api.Type.kind3[CanBuildFrom](Repr, Elem, Repr)
            val cbf = api.Type.inferImplicit(CBF).map(unQualifyStaticModules)
            assert(cbf.isDefined, s"Cannot infer implicit value of type `$CBF`")
            val x = api.ParSym(owner, fresh("x"), Elem)
            val id = src.Lambda(x)(api.ParRef(x))
            (Some(src.DefCall(xs)(FM.map, Elem, Repr)(Seq(id), Seq(cbf.get))), Repr)
          } else (xs, xsTpe)
          val toIter = tpe.member(api.TermName("toIterator")).asTerm
          val Iter = api.Type.result(toIter.infoIn(tpe))
          val iter = api.ValSym(owner, fresh("iter"), Iter)
          val target = Some(src.ValRef(iter))
          val hasNext = Iter.member(api.TermName("hasNext")).asTerm
          val next = Iter.member(api.TermName("next")).asTerm
          val mut = src.VarMut(elem, src.DefCall(target)(next)(Seq.empty))
          src.Block(
            src.ValDef(iter,
              src.DefCall(trav)(toIter)()),
            src.VarDef(elem,
              src.DefCall(Some(src.Lit(null)))(asInst, Elem)()),
            src.While(
              src.DefCall(target)(hasNext)(),
              api.Tree.rename(arg -> elem)(body match {
                case src.Block(stats, expr) => src.Block(mut +: stats: _*)(expr)
                case _ => src.Block(mut, body)()
              })))()
      }.andThen(_.tree)
    }

    /** [[FilterMonadic]]. */
    private object FM {
      val tpe = api.Type[FilterMonadic[Nothing, Nothing]]
      val foreach = tpe.member(api.TermName("foreach")).asMethod
      val map = tpe.member(api.TermName("map")).asMethod
    }
  }

}
