package eu.stratosphere.emma.compiler.lang.source

import eu.stratosphere.emma.compiler.Common

/** Core language. */
trait Source extends Common
  with SourceValidate {

  import universe._

  object Source {

    // -------------------------------------------------------------------------
    // Language
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Validate API
    // -------------------------------------------------------------------------

    /** Delegates to [[SourceValidate.validate()]]. */
    def validate(tree: Tree): Boolean =
      SourceValidate.validate(tree)
  }

}
