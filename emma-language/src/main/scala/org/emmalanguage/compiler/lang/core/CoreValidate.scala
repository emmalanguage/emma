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

/** Validation for the [[Core]] language. */
private[core] trait CoreValidate extends Common {
  self: Core =>

  import Core.{Lang => core}
  import UniverseImplicits._
  import Validation._

  /** Validation for the [[Core]] language. */
  private[core] object CoreValidate {

    /** Fluid [[Validator]] builder. */
    implicit private class Check(tree: u.Tree) {

      /** Provide [[Validator]] to test. */
      case class is(expected: Validator) {

        /** Provide error message in case of validation failure. */
        def otherwise(violation: => String): Verdict =
        validateAs(expected, tree, violation)
      }
    }

    /** Validators for the [[Core]] language. */
    object valid {

      /** Validates that a Scala AST belongs to the supported [[Core]] language. */
      def apply(tree: u.Tree): Verdict = {
        assert(has.tpe(tree), "Core.validate can only be used on typechecked trees.")
        tree is Let otherwise "Unexpected tree (expected let-in block)"
      }

      // ---------------------------------------------------------------------------
      // Atomics
      // ---------------------------------------------------------------------------

      lazy val Lit: Validator = {
        case core.Lit(_) => pass
      }

      lazy val Ref: Validator = {
        case core.Ref(x) if !x.isVar => pass
      }

      lazy val This: Validator = {
        case core.This(_) => pass
      }

      lazy val Atomic: Validator =
        oneOf(Lit, Ref, This)

      // ---------------------------------------------------------------------------
      // Parameters
      // ---------------------------------------------------------------------------

      lazy val ParRef: Validator = {
        case core.ParRef(_) => pass
      }

      lazy val ParDef: Validator = {
        case core.ParDef(_, core.Empty(_)) => pass
      }

      // ---------------------------------------------------------------------------
      // Values
      // ---------------------------------------------------------------------------

      lazy val ValRef: Validator = {
        case core.ValRef(_) => pass
      }

      lazy val ValDef: Validator = {
        case core.ValDef(_, rhs) =>
          rhs is Term otherwise s"Invalid ${core.ValDef} RHS"
      }

      // ---------------------------------------------------------------------------
      // Bindings
      // ---------------------------------------------------------------------------

      lazy val BindingRef: Validator =
        oneOf(ValRef, ParRef)

      lazy val BindingDef: Validator =
        oneOf(ValDef, ParDef)

      // ---------------------------------------------------------------------------
      // Methods
      // ---------------------------------------------------------------------------

      lazy val DefCall: Validator = {
        case core.DefCall(None, _, _, argss) =>
          all (argss.flatten) are Atomic otherwise s"Invalid ${core.DefCall} argument"
        case core.DefCall(Some(target), _, _, argss) => {
          target is Atomic otherwise s"Invalid ${core.DefCall} target"
        } and {
          all (argss.flatten) are Atomic otherwise s"Invalid ${core.DefCall} argument"
        }
      }

      lazy val DefDef: Validator = {
        case core.DefDef(_, _, paramss, body) => {
          all (paramss.flatten) are ParDef otherwise s"Invalid ${core.DefDef} parameter"
        } and {
          body is Let otherwise s"Invalid ${core.DefDef} body"
        }
      }

      // ---------------------------------------------------------------------------
      // Definitions
      // ---------------------------------------------------------------------------

      lazy val TermDef: Validator =
        oneOf(BindingDef, DefDef)

      // ---------------------------------------------------------------------------
      // Terms
      // ---------------------------------------------------------------------------

      lazy val TermAcc: Validator = {
        case core.TermAcc(target, _) =>
          target is Atomic otherwise s"Invalid ${core.TermAcc} target"
      }

      lazy val Branch: Validator = {
        case core.Branch(cond, thn, els) => {
          cond is Atomic otherwise s"Invalid ${core.Branch} condition"
        } and {
          all (thn, els) are oneOf(Atomic, DefCall) otherwise s"Invalid ${core.Branch} expression"
        }
      }

      lazy val Inst: Validator = {
        case core.Inst(_, _, argss) =>
          all (argss.flatten) are Atomic otherwise s"Invalid ${core.Inst} argument"
      }

      lazy val Lambda: Validator = {
        case core.Lambda(_, params, body) => {
          all (params) are ParDef otherwise s"Invalid ${core.Lambda} parameter"
        } and {
          body is Let otherwise s"Invalid ${core.Lambda} body"
        }
      }

      lazy val TypeAscr: Validator = {
        case core.TypeAscr(expr, _) =>
          expr is Atomic otherwise s"Invalid ${core.TypeAscr} expression"
      }

      lazy val Term: Validator =
        oneOf(Atomic, TermAcc, Inst, DefCall, Branch, Lambda, TypeAscr)

      // ---------------------------------------------------------------------------
      // Let-in blocks
      // ---------------------------------------------------------------------------

      lazy val Let: Validator = {
        case core.Let(vals, defs, expr) => {
          all (vals) are ValDef otherwise s"Invalid ${core.Let} binding"
        } and {
          all (defs) are DefDef otherwise s"Invalid ${core.Let} function"
        } and {
          expr is oneOf(Atomic, DefCall, Branch) otherwise s"Invalid ${core.Let} expression"
        }
      }
    }
  }
}
