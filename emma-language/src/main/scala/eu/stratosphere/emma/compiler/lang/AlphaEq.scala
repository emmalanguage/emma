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

    // implicitly convert `alphaEq _` to a tupled version
    implicit def tupled(f: (Tree, Tree) => Eq Or Neq): ((Tree, Tree)) => Eq Or Neq =
      f.tupled

    // provides `failHere` as alias for `fail at (lhs, rhs)`
    lazy val failHere = fail at (lhs, rhs)

    // an accumulator dictionary for alpha equivalences
    val dict = mutable.Map.empty[Symbol, Symbol]

    // -------------------------------------------------------------------------
    // Helper functions
    // -------------------------------------------------------------------------

    def trees(lhs: Tree, rhs: Tree): Eq Or Neq = (lhs, rhs) match {
      case (EmptyTree, EmptyTree) =>
        pass

      case (
        Annotated(annot$l, arg$l),
        Annotated(annot$r, arg$r)
        ) => for {
          eq <- trees(annot$l, annot$r)
          eq <- trees(arg$l, arg$r)
        } yield eq

      case (
        This(qual$l),
        This(qual$r)
        ) => for {
          eq <- symbols(lhs, lhs)
          eq <- names(qual$l, qual$r)
        } yield eq

      case (
        lhs: TypeTree,
        rhs: TypeTree
        ) => for {
          eq <- types(lhs, rhs)
        } yield eq

      case (
        lhs: AppliedTypeTree,
        rhs: AppliedTypeTree
        ) => for {
          eq <- types(lhs, rhs)
        } yield eq

      case (
        lhs: Literal,
        rhs: Literal
        ) => for {
          eq <- literals(lhs, lhs)
        } yield eq

      case (
        lhs: Ident,
        rhs: Ident
        ) => for {
          eq <- symbols(lhs, rhs)
        } yield eq

      case (
        Typed(expr$l, tpt$l),
        Typed(expr$r, tpt$r)
        ) => for {
          eq <- trees(expr$l, expr$r)
          eq <- trees(tpt$l, tpt$r)
        } yield eq

      case (
        Select(qual$l, member$l),
        Select(qual$r, member$r)
        ) => for {
          eq <- trees(qual$l, qual$r)
          eq <- names(member$l, member$r)
        } yield eq

      case (
        Block(stats$l, expr$l),
        Block(stats$r, expr$r)
        ) => for {
          eq <- size(stats$l, stats$r, "block statements")
          eq <- all(lhs.children, rhs.children)
        } yield eq

      case (
        ValDef(mods$l, _, tpt$l, rhs$l),
        ValDef(mods$r, _, tpt$r, rhs$r)
        ) => for {
          eq <- modifiers(mods$l, mods$r)
          eq <- types(tpt$l, tpt$r)
          _ = dict += lhs.symbol -> rhs.symbol
          eq <- trees(rhs$l, rhs$r)
        } yield eq

      case (
        Assign(lhs$l, rhs$l),
        Assign(lhs$r, rhs$r)
        ) => for {
          eq <- trees(lhs$l, lhs$r)
          eq <- trees(rhs$l, rhs$r)
        } yield eq

      case (
        Function(params$l, body$l),
        Function(params$r, body$r)
        ) => for {
          eq <- size(params$l, params$r, "function parameters")
          eq <- all(params$l, params$r)
          _ = dict += lhs.symbol -> rhs.symbol
          eq <- trees(body$l, body$r)
        } yield eq

      case (
        TypeApply(target$l, targs$l),
        TypeApply(target$r, targs$r)
        ) => for {
          eq <- symbols(lhs, rhs)
          eq <- trees(target$l, target$r)
          eq <- size(targs$l, targs$r, "type arguments")
          eq <- all(targs$l, targs$r)
        } yield eq

      case (
        Apply(target$l, args$l),
        Apply(target$r, args$r)
        ) => for {
          eq <- symbols(lhs, rhs)
          eq <- trees(target$l, target$r)
          eq <- size(args$l, args$r, "application arguments")
          eq <- all(args$l, args$r)
        } yield eq

      case (
        New(tpt$l),
        New(tpt$r)
        ) => for {
          eq <- types(tpt$l, tpt$r)
        } yield eq

      case (
        If(cond$l, then$l, else$l),
        If(cond$r, then$r, else$r)
        ) => for {
          eq <- trees(cond$l, cond$r)
          eq <- trees(then$l, then$r)
          eq <- trees(else$l, else$r)
        } yield eq

      case (
        while_(cond$l, body$l),
        while_(cond$r, body$r)
        ) => for {
          eq <- trees(cond$l, cond$r)
          eq <- trees(body$l, body$r)
        } yield eq

      case (
        doWhile(cond$l, body$l),
        doWhile(cond$r, body$r)
        ) => for {
          eq <- trees(cond$l, cond$r)
          eq <- trees(body$l, body$r)
        } yield eq

      case (
        DefDef(mods$l, _, tparams$l, paramss$l, tpt$l, rhs$l),
        DefDef(mods$r, _, tparams$r, paramss$r, tpt$r, rhs$r)
        ) => for {
          eq <- modifiers(mods$l, mods$r)
          eq <- trees(tpt$l, tpt$r)
          eq <- size(tparams$l, tparams$r, "type parameters")
          eq <- all(tparams$l, tparams$r)
          eq <- shape(paramss$l, paramss$r, "method parameters")
          eq <- all(paramss$l.flatten, paramss$r.flatten)
          _ = dict += lhs.symbol -> rhs.symbol
          eq <- trees(rhs$l, rhs$r)
        } yield eq

      case (
        Match(selector$l, cases$l),
        Match(selector$r, cases$r)
        ) => for {
          eq <- trees(selector$l, selector$r)
          eq <- size(cases$l, cases$r, "pattern cases")
          eq <- all(cases$l, cases$r)
        } yield eq

      case (
        CaseDef(pat$l, guard$l, body$l),
        CaseDef(pat$r, guard$r, body$r)
        ) => for {
          eq <- trees(pat$l, pat$r)
          eq <- trees(guard$l, guard$r)
          eq <- trees(body$l, body$r)
        } yield eq

      case (
        Bind(_, body$l),
        Bind(_, body$r)
        ) => for {
          eq <- pass
          _ = dict += lhs.symbol -> rhs.symbol
          eq <- trees(body$l, body$r)
        } yield eq

      case _ =>
        failHere because "Unexpected pair of trees"
    }

    def symbols(lhs: Tree, rhs: Tree): Eq Or Neq = {
      import Term.name.{exprOwner, local}

      def eqSymbols(lhSym: Symbol, rhSym: Symbol): Eq Or Neq = (lhSym, rhSym) match {
        case (_, _) if lhSym == rhSym =>
          pass

        case (lhSym: FreeTermSymbol, rhSym: FreeTermSymbol) => for {
          eq <- {
            if (lhSym.name == lhSym.name) pass
            else failHere because "Free symbol names do not match"
          }
          eq <- {
            if (lhSym.value == lhSym.value) pass
            else failHere because "Free symbol values do not match"
          }
        } yield eq

        case (Term.sym(`exprOwner`, _), Term.sym(`exprOwner`, _)) =>
          pass

        case (Term.sym(`local`, _), Term.sym(`local`, _)) =>
          pass

        case _ => for {
          eq <- {
            if (dict contains lhSym) pass
            else failHere because s"No alpha-mapping for symbol $lhSym available"
          }
          eq <- {
            if (dict(lhSym) == rhSym) pass
            else failHere because s"Symbols $lhSym and $lhSym are not alpha equivalent"
          }
          eq <- eqSymbols(lhSym.owner, rhSym.owner)
        } yield eq
      }

      eqSymbols(lhs.symbol, rhs.symbol)
    }

    def names(lhName: Name, rhName: Name): Eq Or Neq =
      if (lhName == rhName) pass
      else failHere because s"Names $lhName and $rhName are not equal"

    def types(lhs: Tree, rhs: Tree): Eq Or Neq =
      if (lhs.tpe =:= rhs.tpe) pass
      else failHere because s"Types ${lhs.tpe} and ${rhs.tpe} are not equal"

    def literals(lhs: Literal, rhs: Literal): Eq Or Neq =
      if (lhs.value == rhs.value) pass
      else failHere because s"Literals ${lhs.value} and ${rhs.value} are not equal"

    def modifiers(lhMods: Modifiers, rhMods: Modifiers): Eq Or Neq =
      if ((lhMods.flags | Flag.SYNTHETIC) == (rhMods.flags | Flag.SYNTHETIC)) pass
      else failHere because s"$lhMods and $rhMods are not equal"

    def all(lhTrees: Seq[Tree], rhTrees: Seq[Tree]): Eq Or Neq =
      (lhTrees zip rhTrees).foldLeft(pass) { case (result, (x, y)) =>
        if (result.isBad) result else trees(x, y)
      }

    def size(lhTrees: Seq[Tree], rhTrees: Seq[Tree], children: String): Eq Or Neq =
      if (lhTrees.size == rhTrees.size) pass
      else failHere because s"Number of $children does not match"

    def shape(lhTrees: Seq[Seq[Tree]], rhTrees: Seq[Seq[Tree]], children: String): Eq Or Neq = {
      lazy val outerSizeEq = lhTrees.size == rhTrees.size
      lazy val innerSizeEq = (lhTrees zip rhTrees) forall { case (l, r) => l.size == r.size }
      if (outerSizeEq && innerSizeEq) pass
      else failHere because s"Shape of $children does not match"
    }

    trees(lhs, rhs)
  }
}
