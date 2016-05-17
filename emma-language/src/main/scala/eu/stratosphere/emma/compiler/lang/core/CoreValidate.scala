package eu.stratosphere
package emma.compiler
package lang
package core

/** Validation for the Core language. */
private[core] trait CoreValidate extends Common {
  self: Core =>

  import universe._
  import Validation._
  import Core.{Language => core}

  private[core] object CoreValidate {

    implicit private class Is(tree: Tree) {
      case class is(expected: Validator) {
        def otherwise(violation: => String): Verdict =
          validateAs(expected, tree, violation)
      }
    }

    object valid {

      /** Validate that a Scala tree belongs to the supported core language. */
      // TODO: Narrow scope of valid top-level trees
      def apply(tree: Tree): Verdict =
        tree is oneOf(term, lambda, let) otherwise "Unexpected tree"

      lazy val this_ : Validator = {
        case core.this_(_) => pass
      }

      lazy val lit: Validator = {
        case core.lit(_) => pass
      }

      lazy val ref: Validator = {
        case core.ref(_) => pass
      }

      private lazy val pkg: Validator = {
        case core.ref(sym) if sym.isPackage => pass
        case core.qref(qualifier, member) if member.isPackage =>
          qualifier is pkg otherwise "Invalid package qualifier"
      }

      lazy val qref: Validator = {
        case core.qref(qualifier, _) =>
          qualifier is pkg otherwise "Invalid reference qualifier"
      }

      lazy val atomic: Validator =
        oneOf(this_, lit, ref, qref)

      lazy val sel: Validator = { case core.sel(target, _) =>
        target is atomic otherwise "Invalid select target"
      }

      lazy val app: Validator = {
        case core.app(target, _, argss@_*) =>
          all (argss.flatten) are atomic otherwise "Invalid application argument"
      }

      lazy val call: Validator = {
        case core.call(target, _, _, argss@_*) => {
          target is valid.atomic otherwise "Invalid method call target"
        } and {
          all (argss.flatten) are atomic otherwise "Invalid method call argument"
        }
      }

      lazy val inst: Validator = {
        case core.inst(_, _, argss@_*) =>
          all (argss.flatten) are atomic otherwise "Invalid instantiation argument"
      }

      lazy val lambda: Validator = {
        case core.lambda(_, _, body) =>
          body is let otherwise "Invalid lambda function body"
      }

      lazy val typed: Validator = {
        case core.typed(expr, _) =>
          expr is atomic otherwise "Invalid typed expression"
      }

      lazy val if_ : Validator = {
        case core.if_(cond, thn, els) => {
          cond is atomic otherwise "Invalid branch condition"
        } and {
          all (thn, els) are oneOf(atomic, app) otherwise "Invalid branch"
        }
      }

      lazy val term: Validator =
        oneOf(atomic, sel, inst, call, app, lambda, typed)

      lazy val val_ : Validator = {
        case core.val_(_, rhs, _) =>
          rhs is term otherwise "Invalid val rhs"
      }

      lazy val let: Validator = {
        case core.let(vals, defs, expr) => {
          all (vals) are val_ otherwise "Invalid let binding"
        } and {
          all (defs) are def_ otherwise "Invalid let local method"
        } and {
          expr is oneOf(term, if_) otherwise "Invalid let expression"
        }
      }

      lazy val def_ : Validator = {
        case core.def_(_, _, _, body) =>
          body is let otherwise "Invalid method body"
      }
    }
  }
}
