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
package compiler.lang.source

import compiler.Common


/** Source language. */
trait Source extends Common
  with Foreach2Loop
  with PatternMatching
  with SourceValidate {

  // -------------------------------------------------------------------------
  // Source language
  // -------------------------------------------------------------------------

  /** Source language. */
  object Source {

    /**
     * The grammar associated with the [[Lang]] objects and accepted by the [[Source.valid]] method
     * is as follows.
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
     *            | VarRef(target: VarSym)
     *
     * BindingDef = ParDef(lhs: ParSym)
     *            | ValDef(lhs: ValSym, rhs: Term)
     *            | VarDef(lhs: VarSym, rhs: Term)
     *
     * Term = Atomic
     *      | Block(stats: Stat*, expr: Term)
     *      | Branch(cond: Term, thn: Term, els: Term)
     *      | DefCall(target: Term?, method: MethodSym, targs: Type*, argss: Term**)
     *      | Inst(clazz: Type, targs: Type*, argss: Term**)
     *      | Lambda(params: ParDef*, body: Term)
     *      | ModuleAcc(target: Term, member: ModuleSym)
     *      | PatMat(target: Term, cases: Case*)
     *      | TypeAscr(expr: Term, tpe: Type)
     *
     * Loop = DoWhile(cond: Term, body: Stat)
     *      | While(cond: Term, body: Stat)
     *
     * Stat = BindingDef(lhs: BindingSym, rhs: Term)
     *      | Loop(cond: Term, body: Stat)
     *      | VarMut(lhs: VarSym, rhs: Term)
     *      | Term
     *
     * Case = Case(pat: Pat, guard: Term, body: Term)
     *
     * Pat = PatAlt(alternatives: Pat*)
     *     | PatAny
     *     | PatAscr(target: Pat, tpe: Type)
     *     | PatAt(lhs: ValSym, rhs: Pat)
     *     | PatConst(target: TermSym)
     *     | PatLit[A](value: A)
     *     | PatExtr(extr: PatExtr', args: Pat*)
     *     | PatQual(qual: PatQual', member: TermSym)
     *     | PatVar(lhs: ValSym)
     *
     * PatQual' = PatConst(target: TermSym)
     *          | PatQual(qual: PatQual', member: TermSym)
     *
     * PatExtr' = TypeTree(tpe: Type)
     *          | UnApply(qual: PatQual', unApp: MethodSym)
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

      // Variables
      val VarRef = api.VarRef
      val VarDef = api.VarDef
      val VarMut = api.VarMut

      // Modules
      val ModuleRef = api.ModuleRef
      val ModuleAcc = api.ModuleAcc

      // Methods
      val DefCall = api.DefCall

      // Loops
      val Loop    = api.Loop
      val While   = api.While
      val DoWhile = api.DoWhile

      // Patterns
      val Pat     = api.Pat
      val PatCase = api.PatCase
      val PatMat  = api.PatMat

      // Terms
      val Term     = api.Term
      val Block    = api.Block
      val Branch   = api.Branch
      val Inst     = api.Inst
      val Lambda   = api.Lambda
      val TypeAscr = api.TypeAscr

      //@formatter:on
    }

    // -------------------------------------------------------------------------
    // Validation API
    // -------------------------------------------------------------------------

    /** Delegates to [[SourceValidate.valid]]. */
    lazy val valid = SourceValidate.valid

    /** Delegates to [[SourceValidate.valid]]. */
    lazy val validate = (tree: u.Tree) => valid(tree).isGood

    // -------------------------------------------------------------------------
    // Source -> Source transformations API
    // -------------------------------------------------------------------------

    /**
     * Applies the following chain of transformations in order to bring the
     * Source tree into a regular form.
     *
     * - [[PatternMatching.destruct]]
     * - [[Foreach2Loop.transform]]
     */
    lazy val normalize = {
      PatternMatching.destruct
    } andThen {
      Foreach2Loop.transform
    }
  }
}
