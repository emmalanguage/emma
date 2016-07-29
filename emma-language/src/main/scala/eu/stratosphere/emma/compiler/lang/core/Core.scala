package eu.stratosphere
package emma.compiler
package lang.core

import lang.comprehension.Comprehension
import lang.source.Source

/** Core language. */
trait Core extends Common
  with ANF
  with CSE
  with DCE
  with LNF
  with PatternMatching
  with CoreValidate
  with Comprehension { this: Source =>

  import universe._

  /** Core language. */
  object Core {

    // -------------------------------------------------------------------------
    // Core language
    // -------------------------------------------------------------------------

    /**
     * The grammar associated with the [[Lang]] objects and accepted by the [[Core.validate()]]
     * method is as follows.
     *
     * {{{
     * Sym = TermSym
     *     | TypeSym
     *
     * TermSym = BindingSym
     *         | MethodSym
     *         | ModuleSym
     *
     * BindingSym = ParSym
     *            | ValSym
     *            | VarSym
     *
     * Atomic = Lit[A](value: A)
     *        | Ref(target: TermSym)
     *        | This(target: Sym)
     *
     * Ref = BindingRef(target: BindingSym)
     *     | ModuleRef(target: ModuleSym)
     *
     * BindingRef = ParRef(target: ParSym)
     *            | ValRef(target: ValSym)
     *
     * BindingDef = ParDef(lhs: ParSym)
     *            | ValDef(lhs: ValSym, rhs: Term)
     *
     * Expr = Atomic
     *      | DefCall(target: Atomic?, method: MethodSym, targs: Type*, argss: Atomic**)
     *
     * CondExpr = Expr
     *          | Branch(cond: Atomic, thn: Expr, els: Expr)
     *
     * Term = CondExpr
     *      | Inst(clazz: Type, targs: Type*, argss: Atomic**)
     *      | Lambda(params: ParDef*, body: Let)
     *      | ModuleAcc(target: Atomic, member: ModuleSym)
     *      | TypeAscr(expr: Atomic, tpe: Type)
     *
     * DefDef = DefDef(sym: MethodSym, tparams: TypeSym*, paramss: ParDef**, body: Let)
     *
     * Let = Let(vals: ValDef*, defs: DefDef*, expr: CondExpr)
     * }}}
     */
    object Lang {
      //@formatter:off

      // Atomics
      val Atomic = api.Atomic
      val Lit    = api.Lit
      val Ref    = api.TermRef
      val This   = api.This

      // Bindings
      val BindingRef = api.BindingRef
      val BindingDef = api.BindingDef

      // Parameters
      val ParRef = api.ParRef
      val ParDef = api.ParDef

      // Values
      val ValRef = api.ValRef
      val ValDef = api.ValDef

      // Modules
      val ModuleRef = api.ModuleRef
      val ModuleAcc = api.ModuleAcc

      // Methods
      val DefCall = api.DefCall
      val DefDef  = api.DefDef

      // Definitions
      val TermDef = api.TermDef

      // Terms
      val Term     = api.Term
      val Branch   = api.Branch
      val Inst     = api.Inst
      val Lambda   = api.Lambda
      val TypeAscr = api.TypeAscr

      // Let-in blocks
      object Let {

        def apply(vals: u.ValDef*)(defs: u.DefDef*)(expr: u.Tree = Term.unit): u.Block =
          api.Block(vals ++ defs: _*)(expr)

        def unapply(let: u.Block): Option[(Seq[u.ValDef], Seq[u.DefDef], u.Tree)] = let match {
          case api.Block(stats, Term(expr)) => for {
            (vals, stats) <- collectWhile(stats) { case value @ ValDef(_, _, _) => value }
            (defs, Seq()) <- collectWhile(stats) { case defn @ DefDef(_, _, _, _, _) => defn }
          } yield (vals, defs, expr)

          case _ => None
        }

        private def collectWhile[A, B](xs: Seq[A])(pf: A =?> B): Option[(Seq[B], Seq[A])] = {
          val (init, rest) = xs.span(pf.isDefinedAt)
          Some(init.map(pf), rest)
        }
      }

      //@formatter:on
    }

    // -------------------------------------------------------------------------
    // Validate API
    // -------------------------------------------------------------------------

    /** Delegates to [[CoreValidate.valid]]. */
    val valid = CoreValidate.valid

    /** Delegates to [[CoreValidate.valid]]. */
    def validate(tree: u.Tree): Boolean =
      valid(tree).isGood

    // -------------------------------------------------------------------------
    // LNF API
    // -------------------------------------------------------------------------

    /** Delegates to [[LNF.lift()]]. */
    def lift(tree: u.Tree): u.Tree =
      LNF.lift(tree)

    /** Delegates to [[LNF.lower()]]. */
    def lower(tree: u.Tree): u.Tree =
      LNF.lift(tree)

    /** Delegates to [[ANF.resolveNameClashes()]]. */
    def resolveNameClashes(tree: u.Tree): u.Tree =
      ANF.resolveNameClashes(tree)

    /** Delegates to [[ANF.anf()]]. */
    def anf(tree: u.Tree): u.Tree =
      ANF.anf(tree)

    /** Delegates to [[ANF.flatten()]]. */
    def flatten(tree: u.Tree): u.Tree =
      ANF.flatten(tree)

    /** Delegates to [[ANF.simplify()]]. */
    def simplify(tree: u.Tree): u.Tree =
      ANF.simplify(tree)

    // -------------------------------------------------------------------------
    // DCE API
    // -------------------------------------------------------------------------

    /** Delegates to [[DCE.dce()]]. */
    def dce(tree: u.Tree): u.Tree =
      DCE.dce(tree)

    // -------------------------------------------------------------------------
    // CSE API
    // -------------------------------------------------------------------------

    /** Delegates to [[DCE.dce()]]. */
    def cse(tree: u.Tree): u.Tree =
      CSE.cse(tree)

    // -------------------------------------------------------------------------
    // PatternMatching API
    // -------------------------------------------------------------------------

    /** Delegates to [[PatternMatching.destructPatternMatches]]. */
    lazy val destructPatternMatches: u.Tree => u.Tree =
      PatternMatching.destructPatternMatches

    // -------------------------------------------------------------------------
    // Meta Information API
    // -------------------------------------------------------------------------

    /**
     * Provides commonly used meta-information for an input [[u.Tree]].
     *
     * == Assumptions ==
     * - The input [[u.Tree]] is in LNF form.
     */
    class Meta(tree: u.Tree) {

      val defs: Map[u.Symbol, u.ValDef] = tree.collect {
        case value: ValDef if !Is.param(value) =>
          value.symbol -> value
      }.toMap

      val uses: Map[u.Symbol, Int] =
        tree.collect { case id: Ident => id.symbol }
          .view.groupBy(identity)
          .mapValues(_.size)
          .withDefaultValue(0)

      @inline
      def valdef(sym: u.Symbol): Option[u.ValDef] =
        defs.get(sym)

      @inline
      def valuses(sym: u.Symbol): Int =
        uses(sym)
    }
  }
}
