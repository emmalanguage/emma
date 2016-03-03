package eu.stratosphere.emma
package compiler.ir.lnf

import eu.stratosphere.emma.compiler.ir.CommonIR

/** Let-normal form language. */
trait Language extends CommonIR {

  import universe._

  object LNF {

    /** Validate that a Scala [[Tree]] belongs to the supported LNF language. */
    private[emma] def validate(root: Tree): Boolean = root match {
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
      case ValDef(mods, name, tpt, rhs) if !mods.hasFlag(Flag.MUTABLE) =>
        validate(tpt) && validate(rhs)
      case DefDef(mods, name, tparams, vparamss, tpt, rhs) =>
        tparams.forall(validate) && vparamss.flatten.forall(validate) && validate(tpt) && validate(rhs)
      case Function(vparams, body) =>
        vparams.forall(validate) && validate(body)
      case TypeApply(fun, args) =>
        validate(fun)
      case Apply(fun, args) =>
        validate(fun) && args.forall(validate)
      case New(tpt) =>
        validate(tpt)
      case If(cond, thenp, elsep) =>
        validate(cond) && validate(thenp) && validate(elsep)
      case _ =>
        abort(root.pos, s"Unsupported Scala node ${root.getClass} in quoted Emma LNF code")
        false
    }

    /** Check alpha-equivalence of trees. */
    private[emma] def eq(x: Tree, y: Tree)
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
        val xs = stats$x :+ expr$x
        val ys = stats$y :+ expr$y
        val us = xs zip ys

        // collect DefDef pairs visible to all statements within the blocks
        val defDefPairs = us collect {
          case (x@DefDef(_, _, _, _, _, _), y@DefDef(_, _, _, _, _, _)) =>
            (x.symbol, y.symbol)
        }
        // compute a stack of maps to be used at each position of us traversal
        val maps = us.foldLeft(Seq(map ++ defDefPairs))((acc, pair) => pair match {
          case (x@ValDef(_, _, _, _), y@ValDef(_, _, _, _)) =>
            (acc.head + (x.symbol -> y.symbol)) +: acc
          case _ =>
            acc.head +: acc
        }).tail.reverse

        val statsSizeEq = stats$x.size == stats$y.size
        val statsEq = (us zip maps) forall { case ((l, r), m) => eq(l, r)(m) }

        statsSizeEq && statsEq

      case (ValDef(mods$x, _, tpt$x, rhs$x), ValDef(mods$y, _, tpt$y, rhs$y)) =>
        val modsEq = mods$x.flags == mods$y.flags
        val tptEq = eq(tpt$x, tpt$y)
        val rhsEq = eq(rhs$x, rhs$y)
        modsEq && tptEq && rhsEq

      case (DefDef(mods$x, _, tps$x, vpss$x, tpt$x, rhs$x), DefDef(mods$y, _, tps$y, vpss$y, tpt$y, rhs$y)) =>
        val valDefPairs = (vpss$x.flatten zip vpss$y.flatten) collect {
          case (x@ValDef(_, _, _, _), y@ValDef(_, _, _, _)) =>
            (x.symbol, y.symbol)
        }

        val modsEq = mods$x.flags == mods$y.flags
        val tpsSizeEq = tps$x.size == tps$y.size
        val tpsEq = (tps$x zip tps$y) forall { case (l, r) => eq(l, r) }
        val vpssSizeEq = vpss$x.size == vpss$y.size && ((vpss$x zip vpss$y) forall { case (l, r) => l.size == r.size })
        val vpssEq = (vpss$x.flatten zip vpss$y.flatten) forall { case (l, r) => eq(l, r)(map ++ valDefPairs) }
        val tptEq = eq(tpt$x, tpt$y)
        val rhsEq = eq(rhs$x, rhs$y)(map ++ valDefPairs)
        modsEq && tpsSizeEq && tpsEq && vpssSizeEq && vpssEq && tptEq && rhsEq

      case (Function(vparams$x, body$x), Function(vparams$y, body$y)) =>
        lazy val valDefPairs = (vparams$x zip vparams$y) collect {
          case (x@ValDef(_, _, _, _), y@ValDef(_, _, _, _)) =>
            (x.symbol, y.symbol)
        }

        def vparamsSizeEq = vparams$x.size == vparams$y.size
        def vparamsEq = (vparams$x zip vparams$y) forall { case (l, r) => eq(l, r)(map ++ valDefPairs) }
        def bodyEq = eq(body$x, body$y)(map ++ valDefPairs)
        vparamsSizeEq && vparamsEq && bodyEq

      case (TypeApply(fun$x, args$x), TypeApply(fun$y, args$y)) =>
        def symEq = x.symbol == y.symbol
        def funEq = eq(fun$x, fun$y)
        def argsSizeEq = args$x.size == args$y.size
        def argsEq = (args$x zip args$y) forall { case (a$x, a$y) => eq(a$x, a$y) }
        symEq && funEq && argsSizeEq && argsEq

      case (Apply(fun$x, args$x), Apply(fun$y, args$y)) =>
        def symEq = eq(x.symbol, y.symbol)
        def funEq = eq(fun$x, fun$y)
        def argsSizeEq = args$x.size == args$y.size
        def argsEq = (args$x zip args$y) forall { case (a$x, a$y) => eq(a$x, a$y) }
        symEq && funEq && argsSizeEq && argsEq

      case (If(cond$x, thenp$x, elsep$x), If(cond$y, thenp$y, elsep$y)) =>
        def condEq = eq(cond$x, cond$y)
        def thenpEq = eq(thenp$x, thenp$y)
        def elsepEq = eq(elsep$x, elsep$y)
        condEq && thenpEq && elsepEq

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

    /**
     * Lift a Scala core language [[Tree]] into let-normal form.
     *
     * This includes:
     *
     * - bringing the original tree to administrative normal form;
     * - modeling control flow as direct-style;
     * - inlining Emma API expressions.
     *
     * @param tree The core language [[Tree]] to be lifted.
     * @return A direct-style let-normal form variant of the input [[Tree]].
     */
    private[emma] def lift(tree: Tree): Tree = {
      tree
    }

    /**
     * Lower a LNF [[Tree]] back as Emma core language [[Tree]].
     *
     * @param tree A direct-style let-nomal form [[Tree]].
     * @return A Scala [[Tree]] derived by deconstructing the IR tree.
     */
    private[emma] def lower(tree: Tree): Tree = {
      tree
    }

    /**
     * Convert a tree into administrative normal form.
     *
     * This includes:
     *
     * - Introducing dedicated symbols for chains of length greater than one.
     * - Ensuring that all function arguments are trivial identifiers.
     *
     * @param tree The [[Tree]] to be converted.
     * @return An ANF version of the input [[Tree]].
     */
    private[emma] def anf(tree: Tree): Tree = {
      tree
    }
  }

}
