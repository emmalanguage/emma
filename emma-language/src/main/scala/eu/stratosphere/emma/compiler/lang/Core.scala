package eu.stratosphere.emma.compiler.lang

import eu.stratosphere.emma.compiler.Common

import scala.annotation.tailrec
import scala.collection.mutable

/** Let-normal form language. */
trait Core extends Common with Comprehension {

  import Term._
  import Term.name.fresh
  import universe._
  import Tree._

  object Core {

    /** Validate that a Scala [[Tree]] belongs to the supported LNF language. */
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
    def lift(tree: Tree): Tree = {
      tree
    }

    /**
     * Lower a LNF [[Tree]] back as Emma core language [[Tree]].
     *
     * @param tree A direct-style let-nomal form [[Tree]].
     * @return A Scala [[Tree]] derived by deconstructing the IR tree.
     */
    def lower(tree: Tree): Tree = {
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
    def anf(tree: Tree): Tree = {
      assert(nameClashes(tree).isEmpty)
      assert(controlFlowNodes(tree).isEmpty)

      anfTransform(tree)
    }

    private val anfTransform: Tree => Tree = postWalk {
      // Already in ANF
      case EmptyTree => EmptyTree
      case lit: Literal => block(lit)
      case id: Ident if id.isTerm => block(id)
      case value: ValDef if Is param value => value

      case fun: Function =>
        val x = fresh(nameOf(fun))
        val T = Type of fun
        val lhs = Term.sym.free(x, T)
        block(val_(lhs, fun), Term ref lhs)

      case Typed(Block(stats, expr), tpt) =>
        val x = fresh(nameOf(expr))
        val T = Type of tpt
        val lhs = Term.sym.free(x, T)
        val rhs = Type.ascription(expr, T)
        block(stats, val_(lhs, rhs), Term ref lhs)

      case sel@Select(Block(stats, target), _: TypeName) =>
        val x = Type sym sel
        val T = Type of sel
        block(stats, Type.sel(target, x, T))

      case sel@Select(Block(stats, target), member: TermName) =>
        val x = Term sym sel
        val T = Type of sel
        val rhs = Term.sel(target, x, T)
        if (x.isPackage || // Parameter lists follow
          (x.isMethod && x.asMethod.paramLists.nonEmpty) ||
          IR.comprehensionOps.contains(x)) {

          block(stats, rhs)
        } else {
          val lhs = Term.sym.free(fresh(member), T)
          block(stats, val_(lhs, rhs), Term ref lhs)
        }

      case TypeApply(Block(stats, target), types) =>
        val expr = Type.app(target, types map Type.of: _*)
        block(stats, expr)

      case app@Apply(Block(stats, target), args) =>
        if (IR.comprehensionOps contains Term.sym(target)) {
          val expr = Term.app(target)(args)
          block(stats, expr)
        } else {
          val x = fresh(nameOf(target))
          val T = Type of app
          val lhs = Term.sym.free(x, T)
          val init = stats ::: args.flatMap {
            case Block(nested, _) => nested
            case _ => Nil
          }

          val params = args.map {
            case Block(_, expr) => expr
            case arg => arg
          }

          val rhs = Term.app(target)(params)
          // Partially applied multi-arg-list method
          if (Is method Type.of(app)) block(init, rhs)
          else block(init, val_(lhs, rhs), Term ref lhs)
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
      case val_(lhs, Block(stats :+ (int: ValDef), rhs: Ident), flags)
        if int.symbol == rhs.symbol =>
        block(stats, val_(lhs, int.rhs, flags), unit)

      case val_(lhs, Block(stats, rhs), flags) =>
        block(stats, val_(lhs, rhs, flags), unit)
    }

    /** Ensures that all definitions within the `tree` have unique names. */
    def resolveNameClashes(tree: Tree): Tree =
      refresh(tree, nameClashes(tree): _*)

    /** Extracts control flow nodes from the given `tree`. */
    private def controlFlowNodes(tree: Tree): List[Tree] = tree collect {
      case branch: If => branch
      case patMat: Match => patMat
      case dd: DefDef => dd
      case loop: LabelDef => loop
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
        case _: Function => Term.name.lambda
        case _: Literal => Term.name("x")
        case _ => throw new RuntimeException("Unsupported tree")
      }

      loop(tree).encodedName.toString
    }

    /**
     * Eliminates unused valdefs (dead code) from a [[Tree]].
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[Core.anf()]]).
     *
     * == Postconditions ==
     * - All unused valdefs are pruned.
     *
     * @param tree The [[Tree]] to be pruned.
     * @return A [[Tree]] with the same semantics but without unused valdefs.
     */
    def dce(tree: Tree): Tree = {

      // create a mutable local copy of uses to keep track of unused terms
      val meta = new Core.Meta(tree)
      val uses = collection.mutable.Map() ++ meta.uses

      // initialize iteration variables
      var done = false
      var rslt = tree

      val decrementUses: Tree => Unit = traverse {
        case id@Ident(_) if uses.contains(id.symbol) =>
          uses(id.symbol) -= 1
          done = false
      }

      val transform = new postWalk(true) {

        import IR.comprehension

        override def template = {
          // Skip comprehension blocks
          case Block(stats, expr) if !comprehensionChild() =>
            block(
              stats filter {
                case vd@val_(sym, rhs, _) if uses.getOrElse(sym, 0) < 1 =>
                  decrementUses(rhs)
                  false
                case _ =>
                  true
              },
              expr)
        }

        /** Check if the current tree is a child fo a `comprehension` application. */
        private def comprehensionChild(): Boolean = ancestors.headOption.exists {
          case Method.call(_, `comprehension`, _, _) => true
          case _ => false
        }
      }

      while (!done) {
        done = true
        val t = transform(rslt)
        rslt = t
      }

      rslt
    }

    /**
     * Eliminates common subexpressions from a [[Tree]].
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[Core.anf()]]).
     *
     * == Postconditions ==
     * - All common subexpressions and corresponding intermediate values are pruned.
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
              val dict = aliases ++ eq.map(_._1 -> Term.ref(lhs1))
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
        case value: ValDef if !Is.param(value) && value.rhs.nonEmpty =>
          // NOTE: Lazy vals not supported
          assert(!Is.lzy(value))
          Term.sym(value) -> value.rhs
      }

      val dict = loop((vals, Map.empty))
      expr(postWalk(tree) {
        case id: Ident if Has.termSym(id) && dict.contains(id.symbol) =>
          dict(id.symbol)

        case value: ValDef if dict contains value.symbol =>
          unit

        case Block(stats, expr) =>
          // Implicitly removes ()
          block(stats, expr)
      })
    }

    /**
     * Unnests nested blocks [[Tree]].
     *
     * == Preconditions ==
     * - Except the nested blocks, the input tree is in simplified ANF form (see [[Core.anf()]] and [[Core.simplify()]]).
     *
     * == Postconditions ==
     * - A simplified ANF tree where all nested blocks have been flattened.
     *
     * @param tree The [[Tree]] to be normalized.
     * @return A [[Tree]] with the same semantics but without common subexpressions.
     */
    def flatten(tree: Tree): Tree = {

      def hasNestedBlocks(tree: Block): Boolean = {
        val inStats = tree.stats exists {
          case ValDef(_, _, _, _: Block) => true
          case _ => false
        }
        val inExpr = tree.expr match {
          case _: Block => true
          case _ => false
        }
        inStats || inExpr
      }

      postWalk(tree) {
        case parent: Block if hasNestedBlocks(parent) =>
          // flatten (potentially) nested block stats
          val flatStats = parent.stats.flatMap {
            case val_(sym, Block(stats, expr), flags) =>
              stats :+ val_(sym, expr, flags)
            case stat =>
              stat :: Nil
          }
          // flatten (potentially) nested block expr
          val flatExpr = parent.expr match {
            case Block(stats, expr) =>
              stats :+ expr
            case expr =>
              expr :: Nil
          }

          block(flatStats ++ flatExpr)
      }
    }

    /**
     * Inlines `Ident` return expressions in blocks whenever refered symbol is used only once.
     * The resulting [[Tree]] is said to be in ''simplified ANF'' form.
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[Core.anf()]]).
     *
     * == Postconditions ==
     * - `Ident` return expressions in blocks have been inlined whenever possible.
     *
     * @param tree The [[Tree]] to be normalized.
     * @return A [[Tree]] with the same semantics but without common subexpressions.
     */
    def simplify(tree: Tree): Tree = {

      def uses(sym: Symbol, stats: List[Tree]): Boolean = Block(stats, EmptyTree) exists {
        case id: Ident if id.symbol == sym => true
        case _ => false
      }

      def prune(sym: Symbol, stats: List[Tree]): (List[Tree], Option[Tree]) = stats
        .reverse
        .foldLeft((List.empty[Tree], Option.empty[Tree]))((res, tree) => tree match {
          case ValDef(_, _, _, rhs) if tree.symbol == sym =>
            res.copy(_2 = Some(rhs))
          case _ =>
            res.copy(_1 = tree :: res._1)
        })

      postWalk(tree) {
        case bl@Block(stats, expr: Ident) if !uses(expr.symbol, stats) =>
          val (pstats, pexpr) = prune(expr.symbol, stats)
          pexpr.map(expr => block(pstats, expr)).getOrElse(bl)
      }
    }

    /**
     * Tests if `pattern` is irrefutable for the given selector, i.e. if it always matches. If it
     * is, returns a sequence of value definitions equivalent to the bindings in `pattern`.
     * Otherwise returns [[scala.None]].
     *
     * A pattern `p` is irrefutable for type `T` when:
     * - `p` is the wildcard pattern (_);
     * - `p` is a variable pattern;
     * - `p` is a typed pattern `x: U` and `T` is a subtype of `U`;
     * - `p` is an alternative pattern `p1 | ... | pn` and at least one `pi` is irrefutable;
     * - `p` is a case class pattern `c(p1, ..., pn)`, `T` is an instance of `c`, the primary
     * constructor of `c` has argument types `T1, ..., Tn` and each `pi` is irrefutable for `Ti`.
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
        !id.isBackquoted && !id.name.toString.head.isUpper

      pattern match {
        case id: Ident if isVarPattern(id) =>
          Some(Nil)

        case Type.ascription(pat, tpe) if Type.of(sel) weak_<:< tpe =>
          irrefutable(Type.ascription(sel, tpe), pat)

        case bind(lhs, pat) =>
          lazy val value = val_(lhs, sel)
          irrefutable(Term ref lhs, pat).map(value :: _)

        // Alternative patterns don't allow binding
        case Alternative(patterns) =>
          patterns.flatMap(irrefutable(sel, _)).headOption

        case extractor@(_: Apply | _: UnApply) if isCaseClass(extractor) =>
          val args = extractor match {
            case app: Apply => app.args
            case un: UnApply => un.args
          }

          val T = Type of sel
          val clazz = Type.of(extractor).typeSymbol.asClass
          val inst = clazz.primaryConstructor
          val params = inst.infoIn(T).paramLists.head
          val selects = params.map { param =>
            val field = T.member(param.name).asTerm
            Term.sel(sel, field, Type of param)
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
     * - The selector of `mat` is non-null;
     * - `mat` has exactly one irrefutable case;
     * - No guards are used;
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
        assert(cases.nonEmpty, "No cases for pattern match")
        val CaseDef(pat, guard, body) = cases.head
        assert(guard.isEmpty, "Emma does not support guards for pattern matches")
        val T = Type.of(sel)
        val lhs = Term.sym.free(fresh("x"), T)
        val binds = irrefutable(Term ref lhs, pat)
        assert(binds.isDefined, "Unsupported refutable pattern match case detected")
        block(val_(lhs, sel) :: binds.get, body)
    }

    // Avoids blocks without statements
    private def expr(tree: Tree) = tree match {
      case Block(Nil, expr) => expr
      case _ => tree
    }

    /**
     * Provides commonly used meta-information for an input [[Tree]].
     *
     * == Assumptions ==
     * - The input [[Tree]] is in LNF form.
     */
    class Meta(tree: Tree) {

      val defs: Map[Symbol, ValDef] = tree.collect {
        case value: ValDef if !Is.param(value) =>
          value.symbol -> value
      }.toMap

      val uses: Map[Symbol, Int] =
        tree.collect { case id: Ident => id.symbol }
          .view.groupBy(identity)
          .mapValues(_.size)
          .withDefaultValue(0)

      @inline
      def valdef(sym: Symbol): Option[ValDef] =
        defs.get(sym)

      @inline
      def valuses(sym: Symbol): Int =
        uses(sym)
    }

  }

}
