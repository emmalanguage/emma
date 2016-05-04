package eu.stratosphere.emma.compiler.lang.core

import eu.stratosphere.emma.compiler.Common

/** Validation for the Core language. */
private[core] trait CoreValidate extends Common {
  self: Core =>

  import universe._

  private[core] object CoreValidate {

    /** Validate that a Scala [[Tree]] belongs to the supported LNF language. */
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
      case ValDef(mods, name, tpt, rhs) if !mods.hasFlag(Flag.MUTABLE) =>
        validate(tpt) && validate(rhs)
      case DefDef(mods, name, tparams, vparamss, tpt, rhs) =>
        tparams.forall(validate) && vparamss.flatten.forall(validate) && validate(tpt) && validate(rhs)
      case Function(vparams, body) =>
        vparams.forall(validate) && validate(body)
      case TypeApply(fun, args) =>
        validate(fun)
      case Apply(fun, args) =>
        validate(fun) && args.forall(validate)
      case New(tpt) =>
        validate(tpt)
      case If(cond, thenp, elsep) =>
        validate(cond) && validate(thenp) && validate(elsep)
      case _ =>
        abort(tree.pos, s"Unsupported Scala node ${tree.getClass} in quoted Emma LNF code")
        false
    }
  }

}