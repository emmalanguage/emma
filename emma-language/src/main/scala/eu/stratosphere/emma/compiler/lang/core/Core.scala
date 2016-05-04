package eu.stratosphere.emma.compiler.lang.core

import eu.stratosphere.emma.compiler.Common
import eu.stratosphere.emma.compiler.lang.comprehension.Comprehension

/** Core language. */
trait Core extends Common
  with LNF
  with DCE
  with CSE
  with PatternMatching
  with CoreValidate
  with Comprehension {

  import universe._

  object Core {

    // -------------------------------------------------------------------------
    // Language
    // -------------------------------------------------------------------------

    /**
     * The grammar associated with the [[Language]] objects and accepted by the
     * [[Core.validate()]] method is as follows.
     *
     * {{{
     * Atomic  = Null
     *         | Lit[A](v: A)
     *         | Pkg(sym: TypeSymbol)
     *         | This(sym: TermSymbol)
     *         | Ref(sym: TermSymbol)
     *
     * Term    = Atomic
     *         | Sel(target: Atomic, member: TermSymbol)
     *         | App(target: Atomic, targs: Seq[Type], argss: Seq[Seq[Atomic]])
     *         | Call(target: Atomic, method: TermSymbol, targs: Seq[Type], argss: Seq[Seq[Atomic]])
     *         | Inst(target: TypeSymbol, targs: Seq[Type], argss: Seq[Seq[Atomic]])
     *         | Lambda(args: Seq[TermSymbol], body: Let)
     *         | Typed(expr: Atomic, type: Type)
     *
     * Term'   = Term
     *         | If(cond: Atomic, thn: Branch, els: Branch)
     *
     * Branch  = Atomic
     *         | App
     *
     * Val     = (lhs: TermSymbol, rhs: Term)
     *
     * Let     = Let(vals: Seq[Val], defs: Seq[Def], expr: Term')
     *
     * Def     = Def(sym: MethodSymbol, params: TermSymbol, body: Let)
     * }}}
     */
    object Language {
      //@formatter:off

      // -----------------------------------------------------------------------
      // atomics
      // -----------------------------------------------------------------------

      val lit     = Term.lit            // literals
      val ref     = Term.ref            // references

      // -----------------------------------------------------------------------
      // terms
      // -----------------------------------------------------------------------

      // selections
      object sel {
        def apply(target: Tree, member: TermSymbol, tpe: Type = NoType): Select = {
          assert(isAtomic(target), "Selection target is not atomic")
          Term.sel.apply(target, member, tpe)
        }

        def unapply(sel: Select): Option[(Tree, TermSymbol)] =
          Term.sel.unapply(sel)
      }

      // function applications
      object app {
        def apply(target: Tree, targs: Type*)(argss: Seq[Tree]*): Apply = {
          assert(isAtomic(target), "Application target is not atomic")
          assert(argss forall (_ forall isAtomic), "Not all application args are atomic")
          Term.app.apply(target, targs: _*)(argss: _*)
        }

        def unapplySeq(apply: Apply): Option[(Tree, Seq[Type], Seq[Seq[Tree]])] = apply match {
          case Term.app(fun, targs, argss@_*) => fun match {
            case ref(_) => Some(fun, targs, argss)
            case lit(_) => Some(fun, targs, argss)
            case _ => None
          }
          case _ => None
        }
      }

      // method calls
      object call {
        def apply(target: Tree, method: TermSymbol, targs: Type*)(argss: Seq[Tree]*): Tree = {
          assert(isAtomic(target), "Method call target is not atomic or sel")
          assert(argss forall (_ forall isAtomic), "Not all method call args are atomic")
          Method.call.apply(target, method, targs: _*)(argss: _*)
        }

        def unapplySeq(tree: Tree): Option[(Tree, TermSymbol, Seq[Type], Seq[Seq[Tree]])] =
          Method.call.unapplySeq(tree)
      }

      // class instantiations
      object inst {
        def apply(target: TypeSymbol, types: Type*)(argss: Seq[Tree]*): Tree = {
          assert(argss forall (_ forall isAtomic), "Not all instantiation args are atomic")
          Term.inst.apply(target, types: _*)(argss: _*)
        }

        def unapplySeq(tree: Tree): Option[(TypeSymbol, Seq[Type], Seq[Seq[Tree]])] =
          Term.inst.unapplySeq(tree)
      }

      // lambdas
      object lambda {
        def apply(args: TermSymbol*)(body: Block): Function = {
          assert(isLetExpr(body), "Method body is not a let expression")
          Term.lambda.apply(args: _*)(body)
        }

        def unapply(fun: Function): Option[(TermSymbol, Seq[TermSymbol], Block)] = fun match {
          case Term.lambda(sym, args, body: Block) => Some(sym, args, body)
          case _ => None
        }
      }

      // type ascriptions
      object typed {
        def apply(tree: Tree, tpe: Type): Typed = {
          assert(isAtomic(tree), "Selection target is not atomic")
          Type.ascription.apply(tree, tpe)
        }

        def unapply(tpd: Typed): Option[(Tree, Type)] =
          Type.ascription.unapply(tpd)
      }

      // -----------------------------------------------------------------------
      // let expressions
      // -----------------------------------------------------------------------

      // val definitions
      object val_ {
        def apply(lhs: TermSymbol, rhs: Tree = EmptyTree, flags: FlagSet = Flag.SYNTHETIC): ValDef = {
          assert(!isMutable(flags), "ValDef is not immutable")
          Tree.val_.apply(lhs, rhs, flags)
        }

        def unapply(value: ValDef): Option[(TermSymbol, Tree, FlagSet)] = value match {
          case Tree.val_(lhs, rhs, flags) if !isMutable(flags) => Some(lhs, rhs, flags)
          case _ => None
        }

        def isMutable(flags: FlagSet): Boolean =
          (flags | Flag.MUTABLE) == flags
      }

      // let expressions
      object let_ {
        def apply(vals: Seq[ValDef], defs: Seq[DefDef], expr: Tree): Block = {
          assert(isTerm(expr) || isBranch(expr), "Let expr head is not a term or a branch")
          Tree.block.apply(vals ++ defs :+ expr)
        }

        def unapply(block: Block): Option[(List[ValDef], List[DefDef], Tree)] = block match {
          case Tree.block(stats, expr) =>
            val vals = List.newBuilder[ValDef]
            val defs = List.newBuilder[DefDef]

            def expectAll(list: List[Tree]): List[Tree] = list match {
              case (t: ValDef) :: rest => vals += t; expectAll(rest)
              case (t: DefDef) :: rest => defs += t; expectDef(rest)
              case _ => list
            }

            def expectDef(list: List[Tree]): List[Tree] = list match {
              case (t: DefDef) :: rest => defs += t; expectDef(rest)
              case _ => list
            }

            if (expectAll(stats).isEmpty) Some(vals.result(), defs.result(), expr)
            else None
        }
      }

      // -----------------------------------------------------------------------
      // control flow
      // -----------------------------------------------------------------------

      // conditionals
      object if_ {
        def apply(cond: Tree, thn: Tree, els: Tree): If = {
          assert(isAtomic(cond), "If condition is not atomic")
          assert(isAtomic(cond) || isApp(thn), "If then clause is not atomic or function application")
          assert(isAtomic(cond) || isApp(els), "If else clause is not atomic or function application")
          Tree.branch.apply(cond, thn, els)
        }

        def unapply(branch: If): Option[(Tree, Tree, Tree)] =
          Tree.branch.unapply(branch)
      }

      // simple method definitions
      object def_ {
        def apply(sym: MethodSymbol, flags: FlagSet = Flag.SYNTHETIC)(params: TermSymbol*)(body: Block): DefDef = {
          assert(isLetExpr(body), "Method body is not a let expression")
          Method.def_.apply(sym, flags)(params: _*)(body)
        }

        def unapply(dd: DefDef): Option[(MethodSymbol, FlagSet, List[TermSymbol], Block)] =
          Method.def_.unapply(dd)
      }

      //@formatter:on

      private def isAtomic(tree: Tree): Boolean = tree match {
        case ref(_) => true
        case lit(_) => true
        // case pkg(_) => true // TODO
        case _ => false
      }

      private def isTerm(tree: Tree): Boolean = isAtomic(tree) || (tree match {
        case sel(_, _) => true
        case app(_, _, _*) => true
        case call(_, _, _, _*) => true
        case inst(_, _, _*) => true
        case lambda(_, _, _) => true
        case typed(_, _) => true
        case _ => false
      })

      private def isApp(tree: Tree): Boolean = tree match {
        case app(_, _, _*) => true
        case _ => false
      }

      private def isBranch(tree: Tree): Boolean = tree match {
        case if_(_, _, _) => true
        case _ => false
      }

      private def isLetExpr(tree: Tree): Boolean = tree match {
        case let_(_, _, _) => true
        case _ => false
      }
    }

    // -------------------------------------------------------------------------
    // Validate API
    // -------------------------------------------------------------------------

    /** Delegates to [[CoreValidate.validate()]]. */
    def validate(tree: Tree): Boolean =
      CoreValidate.validate(tree)

    // -------------------------------------------------------------------------
    // LNF API
    // -------------------------------------------------------------------------

    /** Delegates to [[LNF.lift()]]. */
    def lift(tree: Tree): Tree =
      LNF.lift(tree)

    /** Delegates to [[LNF.lower()]]. */
    def lower(tree: Tree): Tree =
      LNF.lift(tree)

    /** Delegates to [[LNF.resolveNameClashes()]]. */
    def resolveNameClashes(tree: Tree): Tree =
      LNF.resolveNameClashes(tree)

    /** Delegates to [[LNF.anf()]]. */
    def anf(tree: Tree): Tree =
      LNF.anf(tree)

    /** Delegates to [[LNF.flatten()]]. */
    def flatten(tree: Tree): Tree =
      LNF.flatten(tree)

    /** Delegates to [[LNF.simplify()]]. */
    def simplify(tree: Tree): Tree =
      LNF.simplify(tree)

    // -------------------------------------------------------------------------
    // DCE API
    // -------------------------------------------------------------------------

    /** Delegates to [[DCE.dce()]]. */
    def dce(tree: Tree): Tree =
      DCE.dce(tree)

    // -------------------------------------------------------------------------
    // CSE API
    // -------------------------------------------------------------------------

    /** Delegates to [[DCE.dce()]]. */
    def cse(tree: Tree): Tree =
      CSE.cse(tree)

    // -------------------------------------------------------------------------
    // PatternMatching API
    // -------------------------------------------------------------------------

    /** Delegates to [[PatternMatching.destructPatternMatches]]. */
    val destructPatternMatches: Tree => Tree =
      PatternMatching.destructPatternMatches

    // -------------------------------------------------------------------------
    // Meta Information API
    // -------------------------------------------------------------------------

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
