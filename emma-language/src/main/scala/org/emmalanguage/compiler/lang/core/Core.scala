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
import compiler.ir.DSCFAnnotations._
import compiler.lang.AlphaEq
import compiler.lang.comprehension.Comprehension
import compiler.lang.cf.ControlFlow
import compiler.lang.source.Source

/** Core language. */
private[compiler] trait Core extends Common
  with ANF
  with Comprehension
  with CoreValidate
  with CSE
  with DCE
  with DSCF
  with Pickling
  with Reduce
  with Trampoline {
  self: AlphaEq with Source with ControlFlow =>

  import API._
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

      // Methods
      val DefCall = api.DefCall
      val DefDef  = api.DefDef

      // Terms
      val Term     = api.Term
      val TermAcc  = api.TermAcc
      val TermDef  = api.TermDef
      val Branch   = api.Branch
      val Inst     = api.Inst
      val Lambda   = api.Lambda
      val TypeAscr = api.TypeAscr

      // Let-in blocks
      object Let {
        import ComprehensionSyntax.head

        def apply(
          vals: Seq[u.ValDef] = Seq.empty,
          defs: Seq[u.DefDef] = Seq.empty,
          expr: u.Tree        = Term.unit
        ): u.Block = api.Block(vals ++ defs, expr)

        def unapply(let: u.Block): Option[(Seq[u.ValDef], Seq[u.DefDef], u.Tree)] = let match {
          case api.Block(stats, Term(expr)) if !isComprehensionHead(expr) => for {
            (vals, stats) <- collectWhile(stats) { case value @ ValDef(_, _) => value }
            (defs, Seq()) <- collectWhile(stats) { case defn @ DefDef(_, _, _, _) => defn }
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


      // Matcher for continuations (local method calls and branches)
      object Continuation {
        def unapply(tree: u.Tree): Option[u.Tree] = tree match {
          case DefCall(None, method, _, _) if isCont(method) => Some(tree)
          case Branch(_, DefCall(None, thn, _, _), DefCall(None, els, _, _))
            if isCont(thn) && isCont(els) => Some(tree)
          case _ => None
        }

        private def isCont(method: u.TermSymbol) =
          api.Sym.findAnn[continuation](method).isDefined
      }

      //@formatter:on
    }

    trait Algebra[A] {

      // Empty tree
      def empty: A

      // Atomics
      def lit(value: Any): A
      def this_(encl: u.Symbol): A
      def ref(target: u.TermSymbol): A

      // References (with defaults)
      def bindingRef(target: u.TermSymbol): A = ref(target)
      def valRef(target: u.TermSymbol): A = bindingRef(target)
      def parRef(target: u.TermSymbol): A = bindingRef(target)

      // Definitions
      def bindingDef(lhs: u.TermSymbol, rhs: A): A
      def defDef(sym: u.MethodSymbol, tparams: Seq[u.TypeSymbol], paramss: Seq[Seq[A]], body: A): A

      // Definitions (with defaults)
      def valDef(lhs: u.TermSymbol, rhs: A): A = bindingDef(lhs, rhs)
      def parDef(lhs: u.TermSymbol, rhs: A): A = bindingDef(lhs, rhs)

      // Other
      def typeAscr(target: A, tpe: u.Type): A
      def termAcc(target: A, member: u.TermSymbol): A
      def defCall(target: Option[A], method: u.MethodSymbol, targs: Seq[u.Type], argss: Seq[Seq[A]]): A
      def inst(target: u.Type, targs: Seq[u.Type], argss: Seq[Seq[A]]): A
      def lambda(sym: u.TermSymbol, params: Seq[A], body: A): A
      def branch(cond: A, thn: A, els: A): A
      def let(vals: Seq[A], defs: Seq[A], expr: A): A

      // Comprehensions
      def comprehend(qs: Seq[A], hd: A): A
      def generator(lhs: u.TermSymbol, rhs: A): A
      def guard(expr: A): A
      def head(expr: A): A
    }

    def fold[A](a: Algebra[A])(tree: u.Tree): A = {
      // construct comprehension syntax helper for the given monad
      val cs = Comprehension.Syntax(DataBag.sym)
      def fold(tree: u.Tree): A = tree match {
        // Comprehensions
        case cs.Comprehension(qs, hd) =>
          a.comprehend(qs.map(fold), fold(hd))
        case cs.Generator(lhs, rhs) =>
          a.generator(lhs, fold(rhs))
        case cs.Guard(expr) =>
          a.guard(fold(expr))
        case cs.Head(expr) =>
          a.head(fold(expr))

        // Empty
        case Lang.Empty(_) =>
          a.empty

        // Atomics
        case Lang.Lit(value) =>
          a.lit(value)
        case Lang.This(encl) =>
          a.this_(encl)
        case Lang.ValRef(target) =>
          a.valRef(target)
        case Lang.ParRef(target) =>
          a.parRef(target)
        case Lang.BindingRef(target) =>
          a.bindingRef(target)
        case Lang.Ref(target) =>
          a.ref(target)

        // Definitions
        case Lang.ValDef(lhs, rhs) =>
          a.valDef(lhs, fold(rhs))
        case Lang.ParDef(lhs, rhs) =>
          a.parDef(lhs, fold(rhs))
        case Lang.BindingDef(lhs, rhs) =>
          a.bindingDef(lhs, fold(rhs))
        case Lang.DefDef(sym, tparams, paramss, body) =>
          a.defDef(sym, tparams, paramss.map(_.map(fold)), fold(body))

        // Other
        case Lang.TypeAscr(target, tpe) =>
          a.typeAscr(fold(target), tpe)
        case Lang.TermAcc(target, member) =>
          a.termAcc(fold(target), member)
        case Lang.DefCall(target, method, targs, argss) =>
          a.defCall(target.map(fold), method, targs, argss.map(_.map(fold)))
        case Lang.Inst(target, targs, argss) =>
          a.inst(target, targs, argss.map(_.map(fold)))
        case Lang.Lambda(sym, params, body) =>
          a.lambda(sym, params.map(fold), fold(body))
        case Lang.Branch(cond, thn, els) =>
          a.branch(fold(cond), fold(thn), fold(els))
        case Lang.Let(vals, defs, expr) =>
          a.let(vals.map(fold), defs.map(fold), fold(expr))
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
    lazy val lift = TreeTransform("Core.lift", Seq(
      lnf,
      Comprehension.resugarDataBag,
      Comprehension.normalizeDataBag,
      Reduce.transform
    ))

    /** Chains [[ANF.transform]], and [[DSCF.transform]]. */
    lazy val lnf = TreeTransform("Core.lnf", Seq(
      anf,
      dscf
    ))

    /** Delegates to [[DSCF.transform]]. */
    lazy val dscf = DSCF.transform

    /** Delegates to [[DSCF.inverse]] */
    lazy val dscfInv = DSCF.inverse

    /** Delegates to [[ANF.transform]]. */
    lazy val anf = ANF.transform

    /** Delegates to [[ANF.unnest]]. */
    lazy val unnest = ANF.unnest

    /** Reduce an Emma Core term. */
    lazy val reduce: TreeTransform =
      Reduce.transform

    /** Delegates to [[DSCF.stripAnnotations]]. */
    lazy val stripAnnotations = DSCF.stripAnnotations

    /** Delegates to [[DSCF.mapSuffix]]. */
    def mapSuffix(let: u.Block, res: Option[u.Type] = None)
      (f: (Seq[u.ValDef], u.Tree) => u.Block): u.Block = DSCF.mapSuffix(let, res)(f)

    // -------------------------------------------------------------------------
    // DCE API
    // -------------------------------------------------------------------------

    /** Delegates to [[DCE.transform]]. */
    lazy val dce = DCE.transform

    // -------------------------------------------------------------------------
    // CSE API
    // -------------------------------------------------------------------------

    /** Delegates to [[CSE.transform()]]. */
    lazy val cse: TreeTransform =
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
    // Miscellaneous utilities
    // -------------------------------------------------------------------------

    /**
     * Gets the type argument of the DataBag type that is the type of the given expression.
     */
    def bagElemTpe(xs: u.Tree): u.Type = {
      assert(api.Type.constructor(xs.tpe) =:= DataBag.tpe,
        s"`bagElemTpe` was called with a tree that has a non-bag type. " +
          s"The tree:\n-----\n$xs\n-----\nIts type: `${xs.tpe}`")
      api.Type.arg(1, xs.tpe)
    }
  }

}
