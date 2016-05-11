package eu.stratosphere
package emma.compiler
package lang
package source

/** Validation for the Source language. */
private[source] trait SourceValidate extends Common {
  self: Source =>

  import universe._
  import Validation._
  import Source.{Language => src}

  private[source] object SourceValidate {

    implicit private class Is(tree: Tree) {
      case class is(expected: Validator) {
        def otherwise(violation: => String): Verdict =
          validateAs(expected, tree, violation)
      }
    }

    object valid {

      private val foreach = Type[TraversableOnce[Nothing]]
        .member(Term name "foreach").asTerm

      /** Validate that a Scala tree belongs to the supported source language. */
      def apply(tree: Tree): Verdict =
        tree is valid.term otherwise "Not a term"

      lazy val this_ : Validator = {
        case src.this_(_) => pass
      }

      lazy val lit: Validator = {
        case src.lit(_) => pass
      }

      lazy val ref: Validator = {
        case src.ref(_) => pass
      }

      lazy val atomic: Validator =
        oneOf(this_, lit, ref)

      lazy val sel: Validator = {
        case src.sel(target, _) =>
          target is valid.term otherwise "Invalid select target"
      }

      lazy val app: Validator = {
        case src.app(target, _, argss@_*) => {
          target is valid.term otherwise "Invalid application target"
        } and {
          all (argss.flatten) are valid.term otherwise "Invalid application argument"
        }
      }

      lazy val inst: Validator = {
        case src.inst(_, _, argss@_*) =>
          all (argss.flatten) are valid.term otherwise "Invalid instantiation argument"
      }

      lazy val lambda: Validator = {
        case src.lambda(_, _, body) =>
          body is valid.term otherwise "Invalid lambda function body"
      }

      lazy val typed: Validator = {
        case src.typed(expr, _) =>
          expr is valid.term otherwise "Invalid typed expression"
      }

      lazy val block: Validator = {
        case src.block(stats, expr) => {
          all (stats) are valid.stat otherwise "Invalid block statement"
        } and {
          expr is valid.term otherwise "Invalid last block expression"
        }
      }

      lazy val pattern: Validator = {
        lazy val wildcard: Validator = {
          case Ident(Term.name.wildcard) => pass
        }

        lazy val selPat: Validator = valid.ref orElse {
          case src.sel(target, _) =>
            target is selPat otherwise "Invalid select pattern"
        }

        lazy val appPat: Validator = {
          case src.app(_: TypeTree, Nil, args) =>
            all (args) are valid.pattern otherwise "Invalid extractor subpattern"
          case src.app(target, Nil, args) => {
            target is selPat otherwise "Invalid extractor"
          } and {
            all (args) are valid.pattern otherwise "Invalid extractor subpattern"
          }
        }

        lazy val typedPat: Validator = {
          case src.typed(expr, _) =>
            expr is valid.pattern otherwise "Invalid typed pattern"
        }

        oneOf(wildcard, lit, ref, selPat, appPat, typedPat, bind)
      }

      lazy val bind: Validator = {
        case src.bind(_, rhs) =>
          rhs is valid.pattern otherwise "Invalid binding pattern"
      }

      lazy val case_ : Validator = {
        case src.case_(header, EmptyTree, body) => {
          header is valid.pattern otherwise "Invalid pattern"
        } and {
          body is valid.term otherwise "Invalid case body"
        }
      }

      lazy val match_ : Validator = {
        case src.match_(target, cases) => {
          target is valid.term otherwise "Invalid match target"
        } and {
          all (cases) are valid.case_ otherwise "Invalid case"
        }
      }

      lazy val term: Validator =
        oneOf(atomic, inst, sel, app, lambda, typed, block, branch, match_)

      lazy val val_ : Validator = {
        case src.val_(_, rhs, _) =>
          rhs is valid.term otherwise "Invalid val rhs"
      }

      lazy val assign: Validator = {
        case src.assign(lhs, rhs) => {
          lhs is valid.ref otherwise "Invalid assignment lhs"
        } and {
          rhs is valid.term otherwise "Invalid assignment rhs"
        }
      }

      lazy val branch: Validator = {
        case src.branch(cond, thn, els) => {
          cond is valid.term otherwise "Invalid branch condition"
        } and {
          all (thn, els) are valid.term otherwise "Invalid branch statement"
        }
      }

      lazy val dowhile: Validator = {
        case src.dowhile(cond, body) => {
          cond is valid.term otherwise "Invalid do-while condition"
        } and {
          body is valid.stat otherwise "Invalid do-while body"
        }
      }

      lazy val whiledo: Validator = {
        case src.whiledo(cond, body) => {
          cond is valid.term otherwise "Invalid while condition"
        } and {
          body is valid.stat otherwise "Invalid while body"
        }
      }

      lazy val for_ : Validator = {
        case src.app(src.sel(xs, method), _, Seq(src.lambda(_, Seq(_), body)))
          if method == foreach || method.overrides.contains(foreach) => {
            xs is valid.term otherwise "Invalid for loop generator"
          } and {
            body is valid.stat otherwise "Invalid for loop body"
          }
      }

      lazy val stat: Validator = {
        lazy val blockStat: Validator = {
          case src.block(stats, expr) =>
            all (stats :+ expr) are valid.stat otherwise "Invalid block statement"
        }

        lazy val branchStat: Validator = {
          case src.branch(cond, thn, els) => {
            cond is valid.term otherwise "Invalid branch condition"
          } and {
            all (thn, els) are valid.stat otherwise "Invalid branch term"
          }
        }

        oneOf(assign, val_, blockStat, branchStat, dowhile, whiledo, for_, term)
      }
    }
  }
}
