package eu.stratosphere.emma.compiler.lang

import eu.stratosphere.emma.compiler.Common
import org.scalactic.{Bad, Good, Or}

import scala.collection.mutable

/** Provides alpha equivalence for Scala ASTs. */
trait AlphaEq extends Common {

  import universe._
  import Tree.{while_, doWhile}

  // ---------------------------------------------------------------------------
  // Types
  // ---------------------------------------------------------------------------

  type Eq = Unit

  case class Neq(lhs: Tree, rhs: Tree, msg: String)

  // ---------------------------------------------------------------------------
  // Type constructors
  // ---------------------------------------------------------------------------

  def pass: Eq Or Neq =
    Good(())

  object fail {

    case class at(lhs: Tree, rhs: Tree) {
      def because(msg: String): Eq Or Neq =
        Bad(new Neq(lhs, rhs, msg))
    }

  }

  // -------------------------------------------------------------------------
  // Alpha-equivalence function
  // -------------------------------------------------------------------------

  def alphaEq(lhs: Tree, rhs: Tree): Eq Or Neq = {

    import scala.language.implicitConversions

    /** implicitly convert `alphaEq _` to a tupled version. */
    implicit def tupled(f: (Tree, Tree) => Eq Or Neq): ((Tree, Tree)) => Eq Or Neq =
      f.tupled

    /** Provides `failHere` as alias for `failHere`. */
    lazy val failHere = fail.at(lhs, rhs)

    // an accumulator dictionary for alpha equivalences
    implicit val dict = mutable.Map.empty[Symbol, Symbol]

    // -------------------------------------------------------------------------
    // Helper functions
    // -------------------------------------------------------------------------

    def eqTrees(lhs: Tree, rhs: Tree): Eq Or Neq = {

      (lhs, rhs) match {

        case (EmptyTree, EmptyTree) =>
          pass

        case (
          lhs@Annotated(anot$l, argt$l),
          rhs@Annotated(anot$r, argt$r)
          ) =>
          for {
            anotEq <- eqTrees(anot$l, anot$r)
            argtEq <- eqTrees(argt$l, argt$r)
          } yield pass

        case (
          lhs@This(name$l),
          rhs@This(name$r)
          ) =>
          for {
            symbEq <- eqSymbs(lhs, lhs)
            nameEq <- eqNames(name$l, name$r)(lhs, rhs)
          } yield pass

        case (
          lhs: TypeTree,
          rhs: TypeTree
          ) =>
          for {
            typeEq <- eqTypes(lhs, rhs)
          } yield pass

        case (
          lhs: AppliedTypeTree,
          rhs: AppliedTypeTree
          ) =>
          for {
            typeEq <- eqTypes(lhs, rhs)
          } yield pass

        case (
          lhs: Literal,
          rhs: Literal
          ) =>
          for {
            typeEq <- eqLtrls(lhs, lhs)
          } yield pass

        case (
          lhs: Ident,
          rhs: Ident
          ) =>
          for {
            symbEq <- eqSymbs(lhs, rhs)
          } yield pass

        case (
          lhs@Typed(expr$l, tpet$l),
          rhs@Typed(expr$r, tpet$r)
          ) =>
          for {
            exprEq <- eqTrees(expr$l, expr$l)
            tpetEq <- eqTrees(tpet$l, tpet$r)
          } yield pass

        case (
          lhs@Select(qual$l, name$l),
          rhs@Select(qual$r, name$r)
          ) =>
          for {
            qualEq <- eqTrees(qual$l, qual$r)
            nameEq <- eqNames(name$l, name$r)(lhs, rhs)
          } yield pass

        case (
          lhs@Block(stats$l, expr$l),
          rhs@Block(stats$r, expr$r)
          ) =>
          for {
            sizeEq <- {
              if (stats$l.size == stats$r.size) pass
              else failHere because s"Number of block statements does not match"
            }
            statEq <- eqSeqns(lhs.children, rhs.children)
          } yield pass

        case (
          lhs@ValDef(mods$l, _, tpt$l, rhs$l),
          rhs@ValDef(mods$r, _, tpt$r, rhs$r)
          ) =>
          dict += lhs.symbol -> rhs.symbol
          for {
            modsEq <- eqFlags(mods$l.flags, mods$r.flags)(lhs, rhs)
            typeEq <- eqTypes(tpt$l, tpt$r)
            exprEq <- eqTrees(rhs$l, rhs$r)
          } yield pass

        case (
          lhs@Assign(lhs$l, rhs$l),
          rhs@Assign(lhs$r, rhs$r)
          ) =>
          for {
            lhdsEq <- eqTrees(lhs$l, lhs$r)
            rhdsEq <- eqTrees(rhs$l, rhs$r)
          } yield pass

        case (
          lhs@Function(params$l, body$l),
          rhs@Function(params$r, body$r)
          ) =>
          dict += lhs.symbol -> rhs.symbol
          for {
            sizeEq <- {
              if (params$l.size == params$r.size) pass
              else failHere because s"Number of function parameters does not match"
            }
            parsEq <- eqSeqns(params$l, params$r)
            bodyEq <- eqTrees(body$l, body$r)
          } yield pass

        case (
          lhs@TypeApply(fun$l, types$l),
          rhs@TypeApply(fun$r, types$r)
          ) =>
          for {
            symbEq <- eqSymbs(lhs, rhs)
            funcEq <- eqTrees(fun$l, fun$r)
            sizeEq <- {
              if (types$l.size == types$r.size) pass
              else failHere because s"Number of type parameters does not match"
            }
            typeEq <- eqSeqns(types$l, types$r)
          } yield pass

        case (
          lhs@Apply(fun$l, args$l),
          rhs@Apply(fun$r, args$r)
          ) =>
          for {
            symbEq <- eqSymbs(lhs, rhs)
            funcEq <- eqTrees(fun$l, fun$r)
            sizeEq <- {
              if (args$l.size == args$r.size) pass
              else failHere because s"Number of function arguments does not match"
            }
            argsEq <- eqSeqns(args$l, args$r)
          } yield pass

        case (
          lhs@New(tpt$l),
          rhs@New(tpt$r)
          ) =>
          for {
            typeEq <- eqTypes(tpt$l, tpt$r)
          } yield pass

        case (
          lhs@If(cond$l, then$l, else$l),
          rhs@If(cond$r, then$r, else$r)
          ) =>
          for {
            condEq <- eqTrees(cond$l, cond$r)
            thenEq <- eqTrees(then$l, then$r)
            elseEq <- eqTrees(else$l, else$r)
          } yield pass

        case (
          lhs@while_(cond$l, body$l),
          rhs@while_(cond$r, body$r)
          ) =>
          for {
            condEq <- eqTrees(cond$l, cond$r)
            bodyEq <- eqTrees(body$l, body$r)
          } yield pass

        case (
          lhs@doWhile(cond$l, body$l),
          rhs@doWhile(cond$r, body$r)
          ) =>
          for {
            condEq <- eqTrees(cond$l, cond$r)
            bodyEq <- eqTrees(body$l, body$r)
          } yield pass

        case (
          lhs@DefDef(mods$l, _, tps$l, vpss$l, tpt$l, rhs$l),
          rhs@DefDef(mods$r, _, tps$r, vpss$r, tpt$r, rhs$r)) =>
          dict += lhs.symbol -> rhs.symbol
          for {
            modsEq <- eqFlags(mods$l.flags, mods$r.flags)(lhs, rhs)
            tptsEq <- eqTrees(tpt$l, tpt$r)
            tsizEq <- {
              if (tps$l.size == tps$r.size) pass
              else failHere because s"Number of types does not match"
            }
            typeEq <- eqSeqns(tps$l, tps$r)
            psizEq <- {
              lazy val outerSizeEq = vpss$l.size == vpss$r.size
              lazy val innerSizeEq = (vpss$l zip vpss$r) forall { case (l, r) => l.size == r.size }
              if (innerSizeEq && outerSizeEq) pass
              else failHere because s"Number of parameter lists does not match"
            }
            prmsEq <- eqSeqns(vpss$l.flatten, vpss$r.flatten)
            rhssEq <- eqTrees(rhs$l, rhs$r)
          } yield pass

        case (
          lhs@Match(selector$l, cases$l),
          rhs@Match(selector$r, cases$r)
          ) =>
          for {
            sltrEq <- eqTrees(selector$l, selector$r)
            sizeEq <- {
              if (cases$l.size == cases$r.size) pass
              else failHere because s"Number of cases does not match"
            }
            argsEq <- eqSeqns(cases$l, cases$r)
          } yield pass

        case (
          lhs@CaseDef(pat$l, guard$l, body$l),
          rhs@CaseDef(pat$r, guard$r, body$r)) =>
          for {
            patsEq <- eqTrees(pat$l, pat$r)
            grdsEq <- eqTrees(guard$l, guard$r)
            bodyEq <- eqTrees(body$l, body$r)
          } yield pass

        case (
          lhs@Bind(_, body$l),
          rhs@Bind(_, body$r)) =>
          dict += lhs.symbol -> rhs.symbol
          for {
            exprEq <- eqTrees(body$l, body$r)
          } yield pass

        case _ =>
          failHere because "Unexpected pair of trees"
      }
    }

    def eqSymbs(lhs: Tree, rhs: Tree): Eq Or Neq = {
      import Term.name.{exprOwner, local}

      def eqSymbs(lsm: Symbol, rsm: Symbol): Eq Or Neq = (lsm, rsm) match {
        case (lsm: Symbol, rsm: Symbol) if lsm == rsm =>
          pass

        case (lsm: FreeTermSymbol, rsm: FreeTermSymbol) =>
          for {
            nameEq <- {
              if (lsm.name == lsm.name) pass
              else failHere because "Free symbol names do not match"
            }
            vlueEq <- {
              if (lsm.value == lsm.value) pass
              else failHere because "Free symbol values do not match"
            }
          } yield pass

        case (Term.sym(`exprOwner`, _), Term.sym(`exprOwner`, _)) =>
          pass

        case (Term.sym(`local`, _), Term.sym(`local`, _)) =>
          pass

        case _ =>
          for {
            contEq <- {
              if (dict contains lsm) pass
              else failHere because s"No alpha-mapping for symbol $lsm available"
            }
            symbEq <- {
              if (dict(lsm) == rsm) pass
              else failHere because s"Symbols $lsm and $lsm are not alpha equivalent"
            }
            ownrEq <- eqSymbs(lsm.owner, rsm.owner)
          } yield pass
      }

      eqSymbs(lhs.symbol, rhs.symbol)
    }

    def eqNames(lnm: Name, rnm: Name)(lhs: Tree, rhs: Tree): Eq Or Neq =
      if (lnm == rnm) pass
      else failHere because s"Names $lhs and $rhs are not equal"

    def eqTypes(lhs: Tree, rhs: Tree): Eq Or Neq =
      if (lhs.tpe =:= rhs.tpe) pass
      else failHere because s"Types ${lhs.tpe} and ${rhs.tpe} are not equal"

    def eqLtrls(lhs: Literal, rhs: Literal): Eq Or Neq =
      if (lhs == rhs) pass
      else failHere because "Literals are not equal"

    def eqFlags(lfg: FlagSet, rfg: FlagSet)(lhs: Tree, rhs: Tree): Eq Or Neq =
      if ((lfg | Flag.SYNTHETIC) == (rfg | Flag.SYNTHETIC)) pass
      else failHere because s"Flag sets $lfg and $rfg are not equal"

    def eqSeqns(xs: Seq[Tree], ys: Seq[Tree]): Eq Or Neq =
      (xs zip ys).foldLeft(pass)((res, pair) =>
        if (res.isBad) res
        else (eqTrees _).tupled(pair))

    eqTrees(lhs, rhs)
  }

}
