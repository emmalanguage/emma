package eu.stratosphere.emma
package compiler.lang.core

import compiler.lang.comprehension.Comprehension
import compiler.lang.source.Source
import compiler.Common

/** Core language. */
trait Core extends Common
  with ANF
  with CSE
  with DCE
  with LNF
  with Pickling
  with PatternMatching
  with CoreValidate
  with Comprehension {
  this: Source =>

  import UniverseImplicits._

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

      // Empty
      val Empty  = u.EmptyTree

      // Atomics
      val Atomic = api.Atomic
      val Lit    = api.Lit
      val Ref    = api.TermRef
      val This   = api.This

      // Bindings
      val BindingRef = api.BindingRef // TODO: remove
      val BindingDef = api.BindingDef // TODO: remove

      // Parameters
      val ParRef = api.ParRef // TODO: remove
      val ParDef = api.ParDef

      // Values
      val ValRef = api.ValRef // TODO: remove
      val ValDef = api.ValDef

      // Modules
      val ModuleRef = api.ModuleRef // TODO: remove
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

    abstract class Algebra[A] {

      type S[X] = Seq[X]
      type SS[X] = Seq[Seq[X]]

      //@formatter:off

      // Empty tree
      def empty: A

      // Atomics
      def lit(value: Any): A
      def this_(sym: u.Symbol): A
      def bindingRef(sym: u.TermSymbol): A
      def moduleRef(target: u.ModuleSymbol): A

      // Definitions
      def valDef(lhs: u.TermSymbol, rhs: A, flags: u.FlagSet): A
      def parDef(lhs: u.TermSymbol, rhs: A, flags: u.FlagSet): A
      def defDef(sym: u.MethodSymbol, flags: u.FlagSet, tparams: S[u.TypeSymbol], paramss: SS[A], body: A): A

      // Other
      def typeAscr(target: A, tpe: u.Type): A
      def defCall(target: Option[A], method: u.MethodSymbol, targs: S[u.Type], argss: SS[A]): A
      def inst(target: u.Type, targs: Seq[u.Type], argss: SS[A]): A
      def lambda(sym: u.TermSymbol, params: S[A], body: A): A
      def branch(cond: A, thn: A, els: A): A
      def let(vals: S[A], defs: S[A], expr: A): A

      // Comprehensions
      def comprehend(qs: S[A], hd: A): A
      def generator(lhs: u.TermSymbol, rhs: A): A
      def guard(expr: A): A
      def head(expr: A): A
      def flatten(expr: A): A

      //@formatter:on
    }

    def fold[B](a: Algebra[B])(tree: u.Tree): B = {
      // construct comprehension syntax helper for the given monad
      val cs = new Comprehension.Syntax(API.bagSymbol)

      def fold(tree: u.Tree): B = {
        tree match {

          // Comprehensions
          case cs.Comprehension(qs, hd) =>
            a.comprehend(qs map fold, fold(hd))
          case cs.Generator(lhs, rhs) =>
            a.generator(lhs, fold(rhs))
          case cs.Guard(expr) =>
            a.guard(fold(expr))
          case cs.Head(expr) =>
            a.head(fold(expr))
          case cs.Flatten(expr) =>
            a.flatten(fold(expr))

          // Empty
          case Lang.Empty =>
            a.empty
          // Atomics
          case Lang.Lit(value) =>
            a.lit(value)
          case Lang.This(sym) =>
            a.this_(sym)
          case Lang.ModuleRef(target) =>
            a.moduleRef(target)
          case Lang.BindingRef(sym) =>
            a.bindingRef(sym)

          // Definitions
          case Lang.ValDef(lhs, rhs, flags) =>
            a.valDef(lhs, fold(rhs), flags)
          case Lang.ParDef(lhs, rhs, flags) =>
            a.parDef(lhs, fold(rhs), flags)
          case Lang.DefDef(sym, flags, tparams, paramss, body) =>
            a.defDef(sym, flags, tparams, paramss map (_ map fold), fold(body))

          // Other
          case Lang.TypeAscr(target, tpe) =>
            a.typeAscr(fold(target), tpe)
          case Lang.DefCall(target, method, targs, argss@_*) =>
            a.defCall(target map fold, method, targs, argss map (_ map fold))
          case Lang.Inst(target, targs, argss@_*) =>
            a.inst(target, targs, argss map (_ map fold))
          case Lang.Lambda(sym, params, body) =>
            a.lambda(sym, params map fold, fold(body))
          case Lang.Branch(cond, thn, els) =>
            a.branch(fold(cond), fold(thn), fold(els))
          case Lang.Let(vals, defs, expr) =>
            a.let(vals map fold, defs map fold, fold(expr))
        }
      }

      fold(tree)
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
    // Pickling API
    // -------------------------------------------------------------------------

    /** Delegates to [[Pickle.prettyPrint]]. */
    val prettyPrint = unQualifyStaticModules andThen Pickle.prettyPrint

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
        case value: u.ValDef if !Is.param(value) =>
          value.symbol -> value
      }.toMap

      val uses: Map[u.Symbol, Int] =
        tree.collect { case id: u.Ident => id.symbol }
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
