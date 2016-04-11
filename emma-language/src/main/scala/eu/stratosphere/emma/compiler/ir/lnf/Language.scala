package eu.stratosphere.emma
package compiler.ir.lnf

import compiler.ir.CommonIR

import scala.annotation.tailrec

/** Let-normal form language. */
trait Language extends CommonIR {

  import universe._
  import Tree._

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
        val modsEq = (mods$x.flags | Flag.SYNTHETIC) == (mods$y.flags | Flag.SYNTHETIC)
        val tptEq = eq(tpt$x, tpt$y)
        val rhsEq = eq(rhs$x, rhs$y)
        modsEq && tptEq && rhsEq

      case (DefDef(mods$x, _, tps$x, vpss$x, tpt$x, rhs$x), DefDef(mods$y, _, tps$y, vpss$y, tpt$y, rhs$y)) =>
        val valDefPairs = (vpss$x.flatten zip vpss$y.flatten) collect {
          case (x@ValDef(_, _, _, _), y@ValDef(_, _, _, _)) =>
            (x.symbol, y.symbol)
        }

        val modsEq = (mods$x.flags | Flag.SYNTHETIC) == (mods$y.flags | Flag.SYNTHETIC)
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
     * == Preconditions ==
     *
     * - The input does not contain control flow or function definitions.
     * - There are no name clashes (can be ensured with `resolveNameClashes`).
     *
     * == Postconditions ==
     *
     * - Introduces dedicated symbols for chains of length greater than one.
     * - Ensures that all function arguments are trivial identifiers.
     *
     * @param tree The [[Tree]] to be converted.
     * @return An ANF version of the input [[Tree]].
     */
    // FIXME: What happens if there are methods with by-name parameters?
    private[emma] def anf(tree: Tree): Tree = {
      assert(nameClashes(tree).isEmpty)
      assert(controlFlowNodes(tree).isEmpty)
      // Avoid empty blocks
      expr(anfTransform(tree))
    }

    private val anfTransform: Tree => Tree = postWalk {
      // Already in ANF
      case EmptyTree => EmptyTree
      case lit: Literal => block(lit)
      case id: Ident if id.isTerm => block(id)
      case value: ValDef if isParam(value) => value

      case fun: Function =>
        val name = Term.fresh(nameOf(fun))
        val tpe = Type.of(fun)
        val sym = Term.free(name.toString, tpe)
        block(val_(sym, fun), ref(sym))

      case Typed(Block(stats, expr), tpt) =>
        val tpe = Type.of(tpt)
        val name = Term.fresh(nameOf(expr))
        val lhs = Term.free(name.toString, tpe)
        val rhs = ascribe(expr, tpe)
        block(stats, val_(lhs, rhs), ref(lhs))

      case sel@Select(Block(stats, target), _: TypeName) =>
        val sym = Type.symOf(sel)
        val tpe = Type.of(sel)
        block(stats, Type.sel(target, sym, tpe))

      case sel@Select(Block(stats, target), member: TermName) =>
        val sym = Term.of(sel)
        val tpe = Type.of(sel)
        val rhs = Term.sel(target, sym, tpe)
        if (sym.isPackage || // Parameter lists follow
          (sym.isMethod && sym.asMethod.paramLists.nonEmpty) ||
          IR.comprehensionOps.contains(sym)) {

          block(stats, rhs)
        } else {
          val name = Term.fresh(member).toString
          val lhs = Term.free(name, tpe)
          block(stats, val_(lhs, rhs), ref(lhs))
        }

      case TypeApply(Block(stats, target), types) =>
        val expr = typeApp(target, types.map(Type.of): _*)
        block(stats, expr)

      case app@Apply(Block(stats, target), args) =>
        if (IR.comprehensionOps.contains(Term.of(target))) {
          val expr = Tree.app(target)(args.map(this.expr): _*)
          block(stats, expr)
        } else {
          val name = Term.fresh(nameOf(target))
          val lhs = Term.free(name.toString, Type.of(app))
          val init = stats ::: args.flatMap {
            case Block(nested, _) => nested
            case _ => Nil
          }

          val params = args.map {
            case Block(_, expr) => expr
            case arg => arg
          }

          val rhs = Tree.app(target)(params: _*)
          // Partially applied multi-arg-list method
          if (Type.isMethod(Type of app)) block(init, rhs)
          else block(init, val_(lhs, rhs), ref(lhs))
        }

      // Only if contains nested blocks
      case block: Block if block.children.exists {
        case _: Block => true
        case _ => false
      } =>
        val body = block.children.flatMap {
          case nested: Block => nested.children
          case child => child :: Nil
        }

        // Implicitly removes ()
        Tree.block(body.init, body.last)

      // Avoid duplication of intermediates
      case value@ValDef(mods, _, _, Block(stats :+ (int: ValDef), rhs: Ident))
        if int.symbol == rhs.symbol =>
        val lhs = Term.of(value)
        block(stats, val_(lhs, int.rhs, mods.flags), unit)

      case value@ValDef(mods, _, _, Block(stats, rhs)) =>
        val lhs = Term.of(value)
        block(stats, val_(lhs, rhs, mods.flags), unit)
    }

    /** Ensures that all definitions within the `tree` have unique names. */
    private[emma] def resolveNameClashes(tree: Tree): Tree =
      refresh(tree, nameClashes(tree): _*)

    /** Extracts control flow nodes from the given `tree`. */
    private def controlFlowNodes(tree: Tree): List[Tree] = tree collect {
      case branch: If => branch
      case patMat: Match => patMat
      case dd: DefDef => dd
      case loop@WhileLoop(_, _) => loop
      case loop@DoWhileLoop(_, _) => loop
    }

    /** Returns the set of [[Term]]s in `tree` that have clashing names. */
    private def nameClashes(tree: Tree): Seq[TermSymbol] =
      defs(tree).groupBy(_.name)
        .filter { case (_, defs) => defs.size > 1 }
        .flatMap { case (_, defs) => defs }.toSeq

    /**
     * Returns the encoded name associated with this subtree.
     */
    private def nameOf(tree: Tree): String = {

      @tailrec
      def loop(tree: Tree): Name = tree match {
        case id: Ident => id.name
        case value: ValDef => value.name
        case method: DefDef => method.name
        case Select(_, member) => member
        case Typed(expr, _) => loop(expr)
        case Block(_, expr) => loop(expr)
        case Apply(target, _) => loop(target)
        case TypeApply(target, _) => loop(target)
        case _: Function => Term.lambda
        case _: Literal => Term.name("x")
        case _ => throw new RuntimeException("Unsupported tree")
      }

      loop(tree).encodedName.toString
    }

    /**
     * Eliminates common subexpressions from a [[Tree]].
     *
     * == Preconditions ==
     *  - The input `tree` is in ANF (see [[LNF.anf()]])
     *  - No `lazy val`s are used.
     *
     *  == Postconditions ==
     *   - All common subexpressions and corresponding intermediate values are pruned.
     *
     * @param tree The [[Tree]] to be pruned.
     * @return A [[Tree]] with the same semantics but without common subexpressions.
     */
    def cse(tree: Tree): Tree = {
      type Dict = Map[Symbol, Tree]
      type Subst = (List[(TermSymbol, Tree)], Dict)

      @tailrec
      def loop(subst: Subst): Dict =
        subst match {
          case (Nil, aliases) => aliases
          case ((lhs1, rhs1) :: rest, aliases) =>
            val (eq, neq) = rest.partition { case (lhs2, rhs2) =>
              Type.of(lhs1) =:= Type.of(lhs2) &&
                rhs1.equalsStructure(rhs2)
            }

            val dict = {
              val dict = aliases ++ eq.map(_._1 -> ref(lhs1))
              rhs1 match {
                case lit: Literal => dict + (lhs1 -> lit)
                case _ => dict
              }
            }

            val vals = neq.map { case (lhs, rhs) =>
              lhs -> Tree.subst(rhs, dict)
            }

            loop(vals, dict)
        }

      val vals = tree.collect {
        case vd: ValDef if !isParam(vd) && vd.rhs.nonEmpty =>
          // NOTE: Lazy vals not supported
          assert(!isLazy(vd))
          Term.of(vd) -> vd.rhs
      }

      val dict = loop((vals, Map.empty))
      expr(postWalk(tree) {
        case id: Ident if Has.termSym(id) &&
          dict.contains(id.symbol) =>
            dict(id.symbol)

        case vd: ValDef
          if dict.contains(vd.symbol) =>
            unit

        case Block(stats, expr) =>
          // Implicitly removes ()
          block(stats, expr)
      })
    }

    /**
     * Tests if `pattern` is irrefutable for the given selector, i.e. if it always matches. If it
     * is, returns a sequence of value definitions equivalent to the bindings in `pattern`.
     * Otherwise returns [[scala.None]].
     *
     * A pattern `p` is irrefutable for type `T` when:
     *  - `p` is the wildcard pattern (_);
     *  - `p` is a variable pattern;
     *  - `p` is a typed pattern `x: U` and `T` is a subtype of `U`;
     *  - `p` is an alternative pattern `p1 | ... | pn` and at least one `pi` is irrefutable;
     *  - `p` is a case class pattern `c(p1, ..., pn)`, `T` is an instance of `c`, the primary
     *    constructor of `c` has argument types `T1, ..., Tn` and each `pi` is irrefutable for `Ti`.
     *
     * Caution: Does not consider `null` refutable in contrast to the standard Scala compiler. This
     * might cause [[java.lang.NullPointerException]]s.
     */
    def irrefutable(sel: Tree, pattern: Tree): Option[List[ValDef]] = {
      def isCaseClass(tree: Tree) = {
        val sym = Type.of(tree).typeSymbol
        sym.isClass &&
          sym.asClass.isCaseClass &&
          sym == Type.of(sel).typeSymbol
      }

      def isVarPattern(id: Ident) =
        !id.isBackquoted &&
          !id.name.toString.head.isUpper

      pattern match {
        case id: Ident if isVarPattern(id) =>
          Some(Nil)

        case tp@Typed(pat, _) if Type.of(sel) weak_<:< Type.of(tp) =>
          irrefutable(ascribe(sel, Type.of(tp)), pat)

        case bd@Bind(_, pat) =>
          val sym = Term.of(bd)
          lazy val vd = val_(sym, sel)
          irrefutable(ref(sym), pat).map(vd :: _)

        // Alternative patterns don't allow binding
        case Alternative(patterns) =>
          patterns.flatMap(irrefutable(sel, _)).headOption

        case extractor @ (_: Apply | _: UnApply) if isCaseClass(extractor) =>
          val args = extractor match {
            case app: Apply => app.args
            case un: UnApply => un.args
          }

          val tpe = Type.of(sel)
          val clazz = Type.of(extractor).typeSymbol.asClass
          val inst = clazz.primaryConstructor
          val params = inst.infoIn(tpe).paramLists.head
          val selects = params.map { param =>
            val field = tpe.member(param.name).asTerm
            Term.sel(sel, field, Type.of(param))
          }

          val patterns = selects zip args map (irrefutable _).tupled
          if (patterns.exists(_.isEmpty)) None
          else Some(patterns.flatMap(_.get))

        case _ =>
          None
      }
    }

    /**
     * Eliminates irrefutable pattern matches by replacing them with value definitions corresponding
     * to bindings and field accesses corresponding to case class extractors.
     *
     * == Assumptions ==
     *  - The selector of `mat` is non-null;
     *  - `mat` has exactly one irrefutable case;
     *  - No guards are used;
     *
     * == Example ==
     * {{{
     *   ("life", 42) match {
     *     case (s: String, i: Int) =>
     *       s + i
     *   } \\ <=>
     *   {
     *     val x$1 = ("life", 42)
     *     val s = x$1._1: String
     *     val i = x$1._2: Int
     *     val x$2 = s + i
     *     x$2
     *   }
     * }}}
     */
    val destructPatternMatches: Tree => Tree = postWalk {
      case Match(sel, cases) =>
        assert(cases.size == 1)
        val CaseDef(pat, guard, body) = cases.head
        assert(guard.isEmpty)
        val T = Type.of(sel)
        val name = Term.fresh("x").toString
        val x = Term.free(name, T)
        val binds = irrefutable(ref(x), pat)
        assert(binds.isDefined)
        block(val_(x, sel) :: binds.get, body)
    }

    // Avoids blocks without statements
    private def expr(tree: Tree) = tree match {
      case Block(Nil, expr) => expr
      case _ => tree
    }
  }
}
