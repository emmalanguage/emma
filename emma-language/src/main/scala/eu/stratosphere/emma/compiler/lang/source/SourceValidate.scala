package eu.stratosphere
package emma.compiler
package lang
package source

/** Validation for the [[Source]] language. */
private[source] trait SourceValidate extends Common {
  self: Source =>

  import universe._
  import Validation._
  import Source.{Lang => src}

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

      lazy val Ref: Validator =
        oneOf(BindingRef, ModuleRef)

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
        case src.ParDef(_, api.Tree.empty, _) => pass
      }

      // ---------------------------------------------------------------------------
      // Values
      // ---------------------------------------------------------------------------

      lazy val ValRef: Validator = {
        case src.ValRef(_) => pass
      }

      lazy val ValDef: Validator = {
        case src.ValDef(_, rhs, _) =>
          rhs is Term otherwise s"Invalid ${src.ValDef} RHS"
      }

      // ---------------------------------------------------------------------------
      // Variables
      // ---------------------------------------------------------------------------

      lazy val VarRef: Validator = {
        case src.VarRef(_) => pass
      }

      lazy val VarDef: Validator = {
        case src.VarDef(_, rhs, _) =>
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
      // Modules
      // ---------------------------------------------------------------------------

      lazy val ModuleRef: Validator = {
        case src.ModuleRef(_) => pass
      }

      lazy val ModuleAcc: Validator = {
        case src.ModuleAcc(target, _) =>
          target is Term otherwise s"Invalid ${src.ModuleAcc} target"
      }

      // ---------------------------------------------------------------------------
      // Methods
      // ---------------------------------------------------------------------------

      lazy val DefCall: Validator = {
        case src.DefCall(None, _, _, argss@_*) =>
          all (argss.flatten) are Term otherwise s"Invalid ${src.DefCall} argument"
        case src.DefCall(Some(target), _, _, argss@_*) => {
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
        lazy val Wildcard: Validator = {
          case u.Ident(api.TermName.wildcard) => pass
        }

        lazy val Sel: Validator = valid.Ref orElse {
          case u.Select(target, _) =>
            target is Sel otherwise "Invalid Select pattern"
        }

        lazy val Extractor: Validator = {
          case u.Apply(api.TypeQuote(_), args) =>
            all (args) are Pat otherwise "Invalid Extractor subpattern"
        }

        lazy val Typed: Validator = {
          case u.Typed(expr, _) =>
            expr is Pat otherwise "Invalid Typed pattern"
        }

        oneOf(Lit, Wildcard, Ref, Sel, Extractor, Typed, PatAt)
      }

      lazy val PatAt: Validator = {
        case src.PatAt(_, rhs) =>
          rhs is Pat otherwise s"Invalid ${src.PatAt} pattern"
      }

      lazy val PatCase: Validator = {
        case src.PatCase(pat, api.Tree.empty, body) => {
          pat is Pat otherwise s"Invalid ${src.PatCase} pattern"
        } and {
          body is Term otherwise s"Invalid ${src.PatCase} body"
        }
      }

      lazy val PatMat: Validator = {
        case src.PatMat(target, cases@_*) => {
          target is Term otherwise s"Invalid ${src.PatMat} target"
        } and {
          all (cases) are PatCase otherwise s"Invalid ${src.PatMat} case"
        }
      }

      // ---------------------------------------------------------------------------
      // Terms
      // ---------------------------------------------------------------------------

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
        case src.Inst(_, _, argss@_*) =>
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
        oneOf(Atomic, ModuleAcc, Inst, DefCall, Block, Branch, Lambda, TypeAscr, PatMat)

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
