package eu.stratosphere
package emma.compiler
package lang
package core

import emma.compiler.lang.comprehension.Comprehension

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
     * Atomic  = Lit[A](v: A)
     *         | This(sym: Symbol)
     *         | Ref(sym: TermSymbol)
     *         | QRef(qual: Pkg, sym: TermSymbol)
     *
     * Term    = Atomic
     *         | Sel(tgt: Atomic, member: TermSymbol)
     *         | App(tgt: MethodSymbol, targs: Seq[Type], argss: Seq[Seq[Atomic]])
     *         | Call(tgt: Atomic, method: MethodSymbol, targs: Seq[Type], argss: Seq[Seq[Atomic]])
     *         | Inst(tgt: TypeSymbol, targs: Seq[Type], argss: Seq[Seq[Atomic]])
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

      val this_   = Term.this_          // references to enclosing class / object
      val lit     = Term.lit            // literals
      val ref     = Term.ref            // references

      // qualified references
      object qref {
        def apply(qual: Tree, member: TermSymbol): Select = {
          assert(is pkg qual, "Reference qualifier is not a package")
          Term.sel(qual, member)
        }

        def unapply(sel: Select): Option[(Tree, TermSymbol)] = sel match {
          case Term.sel(qual, member) if is pkg qual => Some(qual, member)
          case _ => None
        }
      }

      // -----------------------------------------------------------------------
      // terms
      // -----------------------------------------------------------------------

      // selections
      object sel {
        def apply(target: Tree, member: TermSymbol, T: Type = NoType): Select = {
          assert(!is.pkg(target), "Use `qref` for qualified references")
          assert(is atomic target, "Selection target is not atomic")
          Term.sel(target, member, T)
        }

        def unapply(sel: Select): Option[(Tree, TermSymbol)] = sel match {
          case Term.sel(target, member) if !is.pkg(target) => Some(target, member)
          case _ => None
        }
      }

      // local method calls
      object app {
        def apply(method: MethodSymbol, targs: Type*)(argss: Seq[Tree]*): Tree = {
          assert(argss.flatten forall is.atomic, "Not all local method call args are atomic")
          Term.app(Term.ref(method), targs: _*)(argss: _*)
        }

        def unapplySeq(app: Apply): Option[(MethodSymbol, Seq[Type], Seq[Seq[Tree]])] =
          app match {
            case Term.app(Term.ref(sym), targs, argss@_*) if sym.isMethod =>
              Some(sym.asMethod, targs, argss)
            case _ => None
          }
      }

      // method calls
      object call {
        def apply(target: Tree, method: TermSymbol, targs: Type*)(argss: Seq[Tree]*): Tree = {
          assert(is atomic target, "Method call target is not atomic")
          assert(argss.flatten forall is.atomic, "Not all method call args are atomic")
          Method.call(target, method, targs: _*)(argss: _*)
        }

        def unapplySeq(tree: Tree): Option[(Tree, TermSymbol, Seq[Type], Seq[Seq[Tree]])] =
          Method.call.unapplySeq(tree)
      }

      // class instantiations
      object inst {
        def apply(clazz: TypeSymbol, types: Type*)(argss: Seq[Tree]*): Tree = {
          assert(argss.flatten forall is.atomic, "Not all instantiation args are atomic")
          Term.inst(clazz, types: _*)(argss: _*)
        }

        def unapplySeq(tree: Tree): Option[(TypeSymbol, Seq[Type], Seq[Seq[Tree]])] =
          Term.inst.unapplySeq(tree)
      }

      // lambdas
      object lambda {
        def apply(args: TermSymbol*)(body: Block): Function = {
          assert(is let body, "Lambda body is not a let expression")
          Term.lambda(args: _*)(body)
        }

        def unapply(fun: Function): Option[(TermSymbol, Seq[TermSymbol], Block)] = fun match {
          case Term.lambda(sym, args, body: Block) => Some(sym, args, body)
          case _ => None
        }
      }

      // type ascriptions
      object typed {
        def apply(expr: Tree, T: Type): Typed = {
          assert(is atomic expr, "Type ascription target is not atomic")
          Type.ascription(expr, T)
        }

        def unapply(typed: Typed): Option[(Tree, Type)] =
          Type.ascription.unapply(typed)
      }

      // -----------------------------------------------------------------------
      // let expressions
      // -----------------------------------------------------------------------

      // val definitions
      object val_ {
        def apply(lhs: TermSymbol,
          rhs: Tree = EmptyTree,
          flags: FlagSet = Flag.SYNTHETIC): ValDef = {

          assert(!is.mutable(flags), "Value definition is mutable")
          Tree.val_(lhs, rhs, flags)
        }

        def unapply(value: ValDef): Option[(TermSymbol, Tree, FlagSet)] = value match {
          case Tree.val_(lhs, rhs, flags) if !is.mutable(flags) => Some(lhs, rhs, flags)
          case _ => None
        }
      }

      // let expressions
      object let {
        def apply(vals: Seq[ValDef], defs: Seq[DefDef], expr: Tree): Block = {
          assert(is.term(expr) || is.branch(expr), "Let expr head is neither term nor branch")
          Tree.block(vals ++ defs, expr)
        }

        def apply(expr: Tree): Block =
          apply(Seq.empty[ValDef], Seq.empty[DefDef], expr)

        def unapply(block: Block): Option[(Seq[ValDef], Seq[DefDef], Tree)] = block match {
          case Tree.block(stats, expr) =>
            val (vals, rest1) = collectWhile(stats) { case value: ValDef => value }
            val (defs, rest2) = collectWhile(rest1) { case method: DefDef => method }
            if (rest2.isEmpty) Some(vals, defs, expr) else None
        }

        private def collectWhile[A, B](xs: Seq[A])(pf: A =?> B): (Seq[B], Seq[A]) = {
          val (init, rest) = xs span pf.isDefinedAt
          (init map pf, rest)
        }
      }

      // -----------------------------------------------------------------------
      // control flow
      // -----------------------------------------------------------------------

      // conditionals
      object if_ {
        def apply(cond: Tree, thn: Tree, els: Tree): If = {
          assert(is.atomic(cond), "If condition is not atomic")
          assert(is.atomic(cond) || is.app(thn),
            "If then clause is not atomic or function application")
          assert(is.atomic(cond) || is.app(els),
            "If else clause is not atomic or function application")
          Tree.branch.apply(cond, thn, els)
        }

        def unapply(branch: If): Option[(Tree, Tree, Tree)] =
          Tree.branch.unapply(branch)
      }

      // simple method definitions
      object def_ {
        def apply(sym: MethodSymbol, flags: FlagSet = Flag.SYNTHETIC)
          (params: TermSymbol*)
          (body: Block): DefDef = {

          assert(is let body, "Method body is not a let expression")
          Method.def_(sym, flags)(params: _*)(body)
        }

        def unapply(definition: DefDef): Option[(MethodSymbol, FlagSet, Seq[TermSymbol], Block)] =
          definition match {
            case Method.def_(sym, flags, args, body: Block) if is let body =>
              Some(sym, flags, args, body)
            case _ => None
          }
      }

      //@formatter:on

      private object is {

        def mutable(flags: FlagSet): Boolean =
          (flags | Flag.MUTABLE) == flags

        def atomic(tree: Tree): Boolean = tree match {
          case lit(_) => true
          case this_(_) => true
          case ref(_) => true
          case qref(_, _) => true
          case _ => false
        }

        def term(tree: Tree): Boolean = atomic(tree) || (tree match {
          case sel(_, _) => true
          case app(_, _, _*) => true
          case call(_, _, _, _*) => true
          case inst(_, _, _*) => true
          case lambda(_, _, _) => true
          case typed(_, _) => true
          case _ => false
        })

        def app(tree: Tree): Boolean = tree match {
          case app(_, _, _*) => true
          case _ => false
        }

        def branch(tree: Tree): Boolean = tree match {
          case if_(_, _, _) => true
          case _ => false
        }

        def let(tree: Tree): Boolean = tree match {
          case let(_, _, _) => true
          case _ => false
        }

        def pkg(tree: Tree): Boolean = tree match {
          case _: This => false // could be package object
          case _ => Has.sym(tree) && tree.symbol.isPackage
        }
      }
    }

    // -------------------------------------------------------------------------
    // Validate API
    // -------------------------------------------------------------------------

    /** Delegates to [[CoreValidate.valid]]. */
    val valid = CoreValidate.valid

    /** Delegates to [[CoreValidate.valid]]. */
    def validate(tree: Tree): Boolean =
      valid(tree).isGood

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

    /** Delegates to [[LNF.foreach2loop()]]. */
    def foreach2loop(tree: Tree): Tree =
      LNF.foreach2loop(tree)

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
