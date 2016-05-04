package eu.stratosphere.emma.compiler.lang.source

import eu.stratosphere.emma.compiler.Common

/** Validation for the Source language. */
private[source] trait SourceValidate extends Common {
  self: Source =>

  import universe._
  import Tree._

  private[source] object SourceValidate {

    /** Validate that a Scala [[Tree]] belongs to the supported core language. */
    def validate(tree: Tree): Boolean = tree match {
      case EmptyTree =>
        true
      case This(qual) =>
        true
      case Literal(Constant(value)) =>
        true
      case Ident(name) =>
        true
      case tpe: TypeTree =>
        tpe.original == null || validate(tpe.original)
      case Annotated(annot, arg) =>
        validate(annot) && validate(arg)
      case AppliedTypeTree(tpt, args) =>
        validate(tpt) && args.forall(validate)
      case Typed(expr, tpt) =>
        validate(expr) && validate(tpt)
      case Select(qualifier, name) =>
        validate(qualifier)
      case Block(stats, expr) =>
        stats.forall(validate) && validate(expr)
      case ValDef(mods, name, tpt, rhs) =>
        validate(tpt) && validate(rhs)
      case Function(vparams, body) =>
        vparams.forall(validate) && validate(body)
      case Assign(lhs, rhs) =>
        validate(lhs) && validate(rhs)
      case TypeApply(fun, args) =>
        validate(fun)
      case Apply(fun, args) =>
        validate(fun) && args.forall(validate)
      case New(tpt) =>
        validate(tpt)
      case If(cond, thenp, elsep) =>
        validate(cond) && validate(thenp) && validate(elsep)
      case while_(cond, body) =>
        validate(cond) && validate(body)
      case doWhile(cond, body) =>
        validate(cond) && validate(body)
      case Match(selector, cases) =>
        validate(selector) && cases.forall(validate)
      case CaseDef(pat, guard, body) =>
        validate(pat) && validate(guard) && validate(body)
      case Bind(name, body) =>
        validate(body)
      case _ =>
        abort(tree.pos, s"Unsupported Scala node ${tree.getClass} in quoted Emma source")
        false
    }
  }

}