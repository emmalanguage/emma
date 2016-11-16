/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package compiler.lang.core

import compiler.Common
import compiler.lang.AlphaEq
import compiler.lang.comprehension.Comprehension
import compiler.lang.source.Source

/** Core language. */
trait Core extends Common
  with ANF
  with Comprehension
  with CoreValidate
  with CSE
  with DCE
  with DSCF
  with Pickling
  with Trampoline {
  this: AlphaEq with Source =>

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
      val Empty = api.Empty

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
        import ComprehensionSyntax.head

        def apply(vals: u.ValDef*)(defs: u.DefDef*)(expr: u.Tree = Term.unit): u.Block =
          api.Block(vals ++ defs: _*)(expr)

        def unapply(let: u.Block): Option[(Seq[u.ValDef], Seq[u.DefDef], u.Tree)] = let match {
          case api.Block(stats, Term(expr)) if !isComprehensionHead(expr) => for {
            (vals, stats) <- collectWhile(stats) { case value @ ValDef(_, _, _) => value }
            (defs, Seq()) <- collectWhile(stats) { case defn @ DefDef(_, _, _, _, _) => defn }
          } yield (vals, defs, expr)

          case _ => None
        }

        private def collectWhile[A, B](xs: Seq[A])(pf: A =?> B): Option[(Seq[B], Seq[A])] = {
          val (init, rest) = xs.span(pf.isDefinedAt)
          Some(init.map(pf), rest)
        }

        private def isComprehensionHead(expr: u.Tree): Boolean = expr match {
          case api.DefCall(_, `head`, _, _) => true
          case _ => false
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
          case Lang.Empty(_) =>
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
    lazy val valid = CoreValidate.valid

    /** Delegates to [[CoreValidate.valid]]. */
    lazy val validate = (tree: u.Tree) => valid(tree).isGood

    // -------------------------------------------------------------------------
    // LNF API
    // -------------------------------------------------------------------------

    /** Lifting. The canonical compiler frontend. */
    lazy val lift: u.Tree => u.Tree = Function.chain(Seq(
      lnf,
      Comprehension.resugar(API.bagSymbol),
      inlineLetExprs,
      Comprehension.normalize(API.bagSymbol),
      uninlineLetExprs))

    /** Chains [[ANF.transform]], and [[DSCF.transform]]. */
    lazy val lnf: u.Tree => u.Tree = anf andThen dscf

    /** Delegates to [[DSCF.transform]]. */
    lazy val dscf = DSCF.transform

    /** Delegates to [[ANF.transform]]. */
    lazy val anf = ANF.transform

    /** Delegates to [[ANF.flatten]]. */
    lazy val flatten = ANF.flatten

    /** Delegates to [[ANF.inlineLetExprs]]. */
    lazy val inlineLetExprs = ANF.inlineLetExprs

    /** Delegates to [[ANF.uninlineLetExprs]]. */
    lazy val uninlineLetExprs = ANF.uninlineLetExprs

    // -------------------------------------------------------------------------
    // DCE API
    // -------------------------------------------------------------------------

    /** Delegates to [[DCE.transform]]. */
    lazy val dce = DCE.transform

    // -------------------------------------------------------------------------
    // CSE API
    // -------------------------------------------------------------------------

    /** Delegates to [[CSE.transform()]]. */
    lazy val cse: u.Tree => u.Tree =
      CSE.transform(Map.empty, Map.empty)

    // -------------------------------------------------------------------------
    // Pickling API
    // -------------------------------------------------------------------------

    /** Delegates to [[Pickle.prettyPrint]]. */
    val prettyPrint = unQualifyStatics andThen Pickle.prettyPrint

    // -------------------------------------------------------------------------
    // Lowering API
    // -------------------------------------------------------------------------

    /** Lowering. The canonical compiler backend. */
    lazy val lower = trampoline

    /** Delegates to [[Trampoline.transform]] */
    lazy val trampoline = Trampoline.transform

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
        case value@Lang.ParDef(symbol, _, _) =>
          symbol -> value
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

    // -------------------------------------------------------------------------
    // Miscellaneous utilities
    // -------------------------------------------------------------------------

    /**
     * Gets the type argument of the DataBag type that is the type of the given expression.
     */
    def bagElemTpe(xs: u.Tree): u.Type = {
      assert(api.Type.constructor(xs.tpe) =:= API.DataBag,
        s"`bagElemTpe` was called with a tree that has a non-bag type. " +
          s"The tree:\n-----\n$xs\n-----\nIts type: `${xs.tpe}`")
      api.Type.arg(1, xs.tpe)
    }
  }

}
