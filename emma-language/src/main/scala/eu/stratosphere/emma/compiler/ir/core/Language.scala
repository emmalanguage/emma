package eu.stratosphere.emma
package compiler.ir.core

import eu.stratosphere.emma.compiler.ir.CommonIR

/** Core language. */
trait Language extends CommonIR {

  import universe._

  object Core {

    /** Validate that a Scala [[Tree]] belongs to the supported core language. */
    private[emma] def validate(root: Tree): Boolean = root match {
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
      case NewIOFormat(apply, implicitArgs) =>
        validate(apply)
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
      case WhileLoop(cond, body) =>
        validate(cond) && validate(body)
      case DoWhileLoop(cond, body) =>
        validate(cond) && validate(body)
      case Match(selector, cases) =>
        validate(selector) && cases.forall(validate)
      case CaseDef(pat, guard, body) =>
        validate(pat) && validate(guard) && validate(body)
      case Bind(name, body) =>
        validate(body)
      case _ =>
        abort(root.pos, s"Unsupported Scala node ${root.getClass} in quoted Emma core code")
        false
    }
  }

}
