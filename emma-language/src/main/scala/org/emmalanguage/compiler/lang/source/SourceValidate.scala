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

/** Validation for the [[Source]] language. */
private[source] trait SourceValidate extends Common {
  self: Source =>

  import Source.{Lang => src}
  import UniverseImplicits._
  import Validation._

  /** Validation for the [[Source]] language. */
  private[source] object SourceValidate {

    /** Fluid [[Validator]] builder. */
    implicit private class Check(tree: u.Tree) {

      /** Provide [[Validator]] to test. */
      case class is(expected: Validator) {

        /** Provide error message in case of validation failure. */
        def otherwise(violation: => String): Verdict =
          validateAs(expected, tree, violation)
      }
    }

    /** Validators for the [[Source]] language. */
    object valid {

      /** Validates that a Scala AST belongs to the supported [[Source]] language. */
      def apply(tree: u.Tree): Verdict =
        tree is valid.Term otherwise "Not a term"

      // ---------------------------------------------------------------------------
      // Atomics
      // ---------------------------------------------------------------------------

      lazy val Lit: Validator = {
        case src.Lit(_) => pass
      }

      lazy val Ref: Validator = {
        case src.Ref(_) => pass
      }

      lazy val This: Validator = {
        case src.This(_) => pass
      }

      lazy val Atomic: Validator =
        oneOf(Lit, This, Ref)

      // ---------------------------------------------------------------------------
      // Parameters
      // ---------------------------------------------------------------------------

      lazy val ParRef: Validator = {
        case src.ParRef(_) => pass
      }

      lazy val ParDef: Validator = {
        case src.ParDef(_, src.Empty(_)) => pass
      }

      // ---------------------------------------------------------------------------
      // Values
      // ---------------------------------------------------------------------------

      lazy val ValRef: Validator = {
        case src.ValRef(_) => pass
      }

      lazy val ValDef: Validator = {
        case src.ValDef(_, rhs) =>
          rhs is Term otherwise s"Invalid ${src.ValDef} RHS"
      }

      // ---------------------------------------------------------------------------
      // Variables
      // ---------------------------------------------------------------------------

      lazy val VarRef: Validator = {
        case src.VarRef(_) => pass
      }

      lazy val VarDef: Validator = {
        case src.VarDef(_, rhs) =>
          rhs is Term otherwise s"Invalid ${src.VarDef} RHS"
      }

      lazy val VarMut: Validator = {
        case src.VarMut(_, rhs) =>
          rhs is Term otherwise s"Invalid ${src.VarMut} RHS"
      }

      // ---------------------------------------------------------------------------
      // Bindings
      // ---------------------------------------------------------------------------

      lazy val BindingRef: Validator =
        oneOf(ValRef, VarRef, ParRef)

      lazy val BindingDef: Validator =
        oneOf(ValDef, VarDef, ParDef)

      // ---------------------------------------------------------------------------
      // Methods
      // ---------------------------------------------------------------------------

      lazy val DefCall: Validator = {
        case src.DefCall(None, _, _, argss) =>
          all (argss.flatten) are Term otherwise s"Invalid ${src.DefCall} argument"
        case src.DefCall(Some(target), _, _, argss) => {
          target is Term otherwise s"Invalid ${src.DefCall} target"
        } and {
          all (argss.flatten) are Term otherwise s"Invalid ${src.DefCall} argument"
        }
      }

      // ---------------------------------------------------------------------------
      // Loops
      // ---------------------------------------------------------------------------

      lazy val While: Validator = {
        case src.While(cond, body) => {
          cond is Term otherwise s"Invalid ${src.While} condition"
        } and {
          body is Stat otherwise s"Invalid ${src.While} body"
        }
      }

      lazy val DoWhile: Validator = {
        case src.DoWhile(cond, body) => {
          cond is Term otherwise s"Invalid ${src.DoWhile} condition"
        } and {
          body is Stat otherwise s"Invalid ${src.DoWhile} body"
        }
      }

      lazy val Loop: Validator =
        oneOf(While, DoWhile)

      // ---------------------------------------------------------------------------
      // Patterns
      // ---------------------------------------------------------------------------

      lazy val Pat: Validator = {
        lazy val Alt: Validator = {
          case api.PatAlt(alternatives) =>
            all (alternatives) are Pat otherwise s"Invalid ${api.PatAlt} alternative"
        }

        lazy val Any: Validator = {
          case api.PatAny(_) => pass
        }

        lazy val Ascr: Validator = {
          case api.PatAscr(target, _) =>
            target is Pat otherwise s"Invalid ${api.PatAscr} target"
        }

        lazy val At: Validator = {
          case api.PatAt(_, rhs) =>
            rhs is Pat otherwise s"Invalid ${api.PatAt} pattern"
        }

        lazy val Const: Validator = {
          case api.PatConst(_) => pass
        }

        lazy val Lit: Validator = {
          case api.PatLit(_) => pass
        }

        lazy val Extr: Validator = {
          case api.PatExtr(_, args) =>
            all (args) are Pat otherwise s"Invalid ${api.PatExtr} argument"
        }

        lazy val Qual: Validator = Ref orElse {
          case api.PatQual(qual, _) =>
            qual is Qual otherwise s"Invalid ${api.PatQual} qualifier"
        }

        lazy val Var: Validator = {
          case api.PatVar(_) => pass
        }

        oneOf(Alt, Any, Ascr, At, Const, Lit, Extr, Qual, Var)
      }

      lazy val PatCase: Validator = {
        case src.PatCase(pat, src.Empty(_), body) => {
          pat is Pat otherwise s"Invalid ${src.PatCase} pattern"
        } and {
          body is Term otherwise s"Invalid ${src.PatCase} body"
        }
      }

      lazy val PatMat: Validator = {
        case src.PatMat(target, cases) => {
          target is Term otherwise s"Invalid ${src.PatMat} target"
        } and {
          all (cases) are PatCase otherwise s"Invalid ${src.PatMat} case"
        }
      }

      // ---------------------------------------------------------------------------
      // Terms
      // ---------------------------------------------------------------------------

      lazy val TermAcc: Validator = {
        case src.TermAcc(target, _) =>
          target is Term otherwise s"Invalid ${src.TermAcc} target"
      }

      lazy val Block: Validator = {
        case src.Block(stats, expr) => {
          all (stats) are Stat otherwise s"Invalid ${src.Block} statement"
        } and {
          expr is Term otherwise s"Invalid last ${src.Block} expression"
        }
      }

      lazy val Branch: Validator = {
        case src.Branch(cond, thn, els) => {
          cond is Term otherwise s"Invalid ${src.Branch} condition"
        } and {
          all (thn, els) are Term otherwise s"Invalid ${src.Branch} expression"
        }
      }

      lazy val Inst: Validator = {
        case src.Inst(_, _, argss) =>
          all (argss.flatten) are Term otherwise s"Invalid ${src.Inst} argument"
      }

      lazy val Lambda: Validator = {
        case src.Lambda(_, params, body) => {
          all (params) are ParDef otherwise s"Invalid ${src.Lambda} parameter"
        } and {
          body is Term otherwise s"Invalid ${src.Lambda} body"
        }
      }

      lazy val TypeAscr: Validator = {
        case src.TypeAscr(expr, _) =>
          expr is Term otherwise s"Invalid ${src.TypeAscr} expression"
      }

      lazy val Term: Validator =
        oneOf(Atomic, TermAcc, Inst, DefCall, Block, Branch, Lambda, TypeAscr, PatMat)

      // ---------------------------------------------------------------------------
      // Statements
      // ---------------------------------------------------------------------------

      lazy val For: Validator = {
        case src.DefCall(Some(xs), method, _, Seq(src.Lambda(_, Seq(_), body)))
          if method == api.Sym.foreach || method.overrides.contains(api.Sym.foreach) => {
            xs is Term otherwise "Invalid For loop generator"
          } and {
            body is Term otherwise "Invalid For loop body"
          }
      }

      lazy val Stat: Validator =
        oneOf(ValDef, VarDef, VarMut, Loop, For, Term)
    }
  }
}
