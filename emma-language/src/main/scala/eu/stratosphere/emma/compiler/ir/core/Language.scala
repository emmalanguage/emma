package eu.stratosphere.emma
package compiler.ir.core

import eu.stratosphere.emma.compiler.ir.CommonIR

/** Core language. */
trait Language extends CommonIR {

  import universe._

  object Core {

    /** Validate that a Scala [[Tree]] belongs to the supported core language. */
    def validate(root: Tree): Boolean = root match {
      case EmptyTree =>
        true
      case This(qual) =>
        true
      case Literal(Constant(value)) =>
        true
      case Ident(name) =>
        true
      case tpe: TypeTree =>
        tpe.original == null || validate(tpe.original)
      case NewIOFormat(apply, implicitArgs) =>
        validate(apply)
      case Annotated(annot, arg) =>
        validate(annot) && validate(arg)
      case AppliedTypeTree(tpt, args) =>
        validate(tpt) && args.forall(validate)
      case Typed(expr, tpt) =>
        validate(expr) && validate(tpt)
      case Select(qualifier, name) =>
        validate(qualifier)
      case Block(stats, expr) =>
        stats.forall(validate) && validate(expr)
      case ValDef(mods, name, tpt, rhs) =>
        validate(tpt) && validate(rhs)
      case Function(vparams, body) =>
        vparams.forall(validate) && validate(body)
      case Assign(lhs, rhs) =>
        validate(lhs) && validate(rhs)
      case TypeApply(fun, args) =>
        validate(fun)
      case Apply(fun, args) =>
        validate(fun) && args.forall(validate)
      case New(tpt) =>
        validate(tpt)
      case If(cond, thenp, elsep) =>
        validate(cond) && validate(thenp) && validate(elsep)
      case WhileLoop(cond, body) =>
        validate(cond) && validate(body)
      case DoWhileLoop(cond, body) =>
        validate(cond) && validate(body)
      case Match(selector, cases) =>
        validate(selector) && cases.forall(validate)
      case CaseDef(pat, guard, body) =>
        validate(pat) && validate(guard) && validate(body)
      case Bind(name, body) =>
        validate(body)
      case _ =>
        abort(root.pos, s"Unsupported Scala node ${root.getClass} in quoted Emma core code")
        false
    }

    /** Check alpha-equivalence of trees. */
    def eq(x: Tree, y: Tree)
      (implicit map: Map[Symbol, Symbol] = Map.empty[Symbol, Symbol]): Boolean = (x, y) match {

      case (NewIOFormat(apply$x, implicitArgs$x), NewIOFormat(apply$y, implicitArgs$y)) =>
        eq(apply$x, apply$y)

      case (EmptyTree, EmptyTree) =>
        true

      case (Annotated(annot$x, arg$x), Annotated(annot$y, arg$y)) =>
        def annotEq = eq(annot$x, annot$y)
        def argEq = eq(arg$x, arg$y)
        annotEq && argEq

      case (This(qual$x), This(qual$y)) =>
        qual$x == qual$y && x.symbol == y.symbol

      case (AppliedTypeTree(tpt$x, args$x), AppliedTypeTree(tpt$y, args$y)) =>
        x.tpe =:= y.tpe

      case (tpe$x: TypeTree, tpe$y: TypeTree) =>
        tpe$x.tpe =:= tpe$y.tpe

      case (Literal(Constant(value$x)), Literal(Constant(value$y))) =>
        value$x == value$y

      case (Ident(name$x), Ident(name$y)) =>
        eq(x.symbol, y.symbol)

      case (Typed(expr$x, tpt$x), Typed(expr$y, tpt$y)) =>
        def exprEq = eq(expr$x, expr$x)
        def tptEq = eq(tpt$x, tpt$y)
        exprEq && tptEq

      case (Select(qualifier$x, name$x), Select(qualifier$y, name$y)) =>
        def qualifierEq = eq(qualifier$x, qualifier$y)
        def nameEq = name$x == name$y
        qualifierEq && nameEq

      case (Block(stats$x, expr$x), Block(stats$y, expr$y)) =>
        lazy val xs = stats$x :+ expr$x
        lazy val ys = stats$y :+ expr$y
        lazy val us = xs zip ys

        // compute a stack of maps to be used at each position of us traversal
        lazy val maps = us.foldLeft(Seq(map))((acc, pair) => pair match {
          case (x@ValDef(_, _, _, _), y@ValDef(_, _, _, _)) =>
            (acc.head + (x.symbol -> y.symbol)) +: acc
          case _ =>
            acc.head +: acc
        }).tail.reverse

        def statsSizeEq = stats$x.size == stats$y.size
        def statsEq = (us zip maps) forall { case ((l, r), m) => eq(l, r)(m) }

        statsSizeEq && statsEq

      case (ValDef(mods$x, _, tpt$x, rhs$x), ValDef(mods$y, _, tpt$y, rhs$y)) =>
        def modsEq = mods$x.flags == mods$y.flags
        def tptEq = eq(tpt$x, tpt$y)
        def rhsEq = eq(rhs$x, rhs$y)
        modsEq && tptEq && rhsEq

      case (Function(vparams$x, body$x), Function(vparams$y, body$y)) =>
        lazy val valDefPairs = (vparams$x zip vparams$y) collect {
          case (x@ValDef(_, _, _, _), y@ValDef(_, _, _, _)) =>
            (x.symbol, y.symbol)
        }

        def vparamsSizeEq = vparams$x.size == vparams$y.size
        def vparamsEq = (vparams$x zip vparams$y) forall { case (l, r) => eq(l, r)(map ++ valDefPairs) }
        def bodyEq = eq(body$x, body$y)(map ++ valDefPairs)
        vparamsSizeEq && vparamsEq && bodyEq

      case (Assign(lhs$x, rhs$x), Assign(lhs$y, rhs$y)) =>
        def lhsEq = eq(lhs$x, lhs$y)
        def rhsEq = eq(rhs$x, rhs$y)
        lhsEq && rhsEq

      case (TypeApply(fun$x, args$x), TypeApply(fun$y, args$y)) =>
        def symEq = x.symbol == y.symbol
        def funEq = eq(fun$x, fun$y)
        def argsSizeEq = args$x.size == args$y.size
        def argsEq = (args$x zip args$y) forall { case (a$x, a$y) => eq(a$x, a$y) }
        symEq && funEq && argsSizeEq && argsEq

      case (Apply(fun$x, args$x), Apply(fun$y, args$y)) =>
        def symEq = x.symbol == y.symbol
        def funEq = eq(fun$x, fun$y)
        def argsSizeEq = args$x.size == args$y.size
        def argsEq = (args$x zip args$y) forall { case (a$x, a$y) => eq(a$x, a$y) }
        symEq && funEq && argsSizeEq && argsEq

      case (If(cond$x, thenp$x, elsep$x), If(cond$y, thenp$y, elsep$y)) =>
        def condEq = eq(cond$x, cond$y)
        def thenpEq = eq(thenp$x, thenp$y)
        def elsepEq = eq(elsep$x, elsep$y)
        condEq && thenpEq && elsepEq

      case (WhileLoop(cond$x, body$x), WhileLoop(cond$y, body$y)) =>
        def condEq = eq(cond$x, cond$y)
        def bodyEq = eq(body$x, body$y)
        condEq && bodyEq

      case (DoWhileLoop(cond$x, body$x), DoWhileLoop(cond$y, body$y)) =>
        def condEq = eq(cond$x, cond$y)
        def bodyEq = eq(body$x, body$y)
        condEq && bodyEq

      case (Match(selector$x, cases$x), Match(selector$y, cases$y)) =>
        def selectorEq = eq(selector$x, selector$y)
        def casesSizeEq = cases$x.size == cases$y.size
        def casesEq = (cases$x zip cases$y) forall { case (c$x, c$y) => eq(c$x, c$y) }
        selectorEq && casesSizeEq && casesEq

      case (CaseDef(pat$x, guard$x, body$x), CaseDef(pat$y, guard$y, body$y)) =>
        // convert pat$x  and pat$y as tree sequences
        lazy val pat$x$seq = pat$x collect { case t => t }
        lazy val pat$y$seq = pat$y collect { case t => t }

        // collect Bind pairs visible to the guard and the body of the patterns
        lazy val bindPairs = (pat$x$seq zip pat$y$seq) collect {
          case (bind$x@Bind(_, _), bind$y@Bind(_, _)) => (bind$x.symbol, bind$y.symbol)
        }

        def patSizeEq = pat$x$seq.size == pat$y$seq.size
        def patEq = eq(pat$x, pat$y)
        def guardEq = eq(guard$x, guard$y)(map ++ bindPairs)
        def bodyEq = eq(body$x, body$y)(map ++ bindPairs)
        patSizeEq && patEq && guardEq && bodyEq

      case (Bind(name$x, body$x), Bind(name$y, body$y)) =>
        eq(body$x, body$y)

      case _ =>
        false
    }

    /** Check symbol equivalence. */
    private def eq(x: Symbol, y: Symbol)(implicit map: Map[Symbol, Symbol]): Boolean = (x, y) match {
      case (x: FreeTermSymbol, y: FreeTermSymbol) =>
        x.value == y.value
      case _ =>
        if (map.contains(x)) map(x) == y
        else x == y
    }
  }

}
