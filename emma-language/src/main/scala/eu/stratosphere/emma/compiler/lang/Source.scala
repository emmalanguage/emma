package eu.stratosphere.emma.compiler.lang

import eu.stratosphere.emma.compiler.Common

/** Core language. */
trait Source extends Common {

  import universe._
  import Tree._

  object Source {

    /** Validate that a Scala [[Tree]] belongs to the supported core language. */
    def validate(root: Tree): Boolean = root match {
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
        abort(root.pos, s"Unsupported Scala node ${root.getClass} in quoted Emma source")
        false
    }

    object Language {
      //@formatter:off

      // atomics
      val lit     = Term.lit            // literals
      val ref     = Term.ref            // references

      // terms
      val sel     = Term.sel            // selections
      val app     = Term.app            // function applications
      val inst    = Term.inst           // class instantiation
      val lambda  = Term.lambda         // lambdas
      val typed   = Type.ascription     // type ascriptions

      // state
      val val_    = Tree.val_           // val and var definitions
      val assign  = Tree.assign         // assignments
      val block   = Tree.block          // blocks

      // control flow
      val branch  = Tree.branch         // conditionals
      val whiledo = Tree.while_         // while loop
      val dowhile = Tree.doWhile        // do while loop

      // pattern matching
      val match_  = Tree.match_         // pattern match
      val case_   = Tree.case_          // case
      val bind    = Tree.bind           // bind

      //@formatter:on
    }

  }

}
