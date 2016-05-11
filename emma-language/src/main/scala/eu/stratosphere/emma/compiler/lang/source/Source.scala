package eu.stratosphere
package emma.compiler
package lang
package source

/** Core language. */
trait Source extends Common
  with SourceValidate {

  import universe._

  object Source {

    // -------------------------------------------------------------------------
    // Language
    // -------------------------------------------------------------------------

    /**
     * The grammar associated with the [[Language]] objects and accepted by the
     * [[Source.valid]] method is as follows.
     *
     * {{{
     *
     * Atomic  = This
     *         | Lit[A](v: A)
     *         | Ref(sym: TermSymbol)
     *
     * Term    = Atomic
     *         | Sel(target: Term', member: TermSymbol)
     *         | App(target: Term', targs: Seq[Type], argss: Seq[Seq[Term']])
     *         | Inst(target: TypeSymbol, targs: Seq[Type], argss: Seq[Seq[Term']])
     *         | Lambda(args: Seq[TermSymbol], body: Term')
     *         | Typed(expr: Term', type: Type)
     *
     * Term'   = Term
     *         | Block(stats: Seq[Stat'], expr: Term')
     *         | Branch(cond: Term, thn: Term, els: Term)
     *         | Match(target: Term', cases: Seq[Case])
     *
     * Stat    = Val(lhs: TermSymbol, rhs: Term')
     *         | Assign(lhs: Term', rhs: Term')
     *
     * Stat'   = Stat
     *         | Term'
     *         | Block'(stats: Seq[Stat'])
     *         | Branch'(cond: Term', thn: Stat', els: Stat')
     *         | DoWhile(cond: Term', body: Stat')
     *         | WhileDo(cond: Term', body: Stat')
     *         | For(i: TermSymbol, range: Range, body: Stat')
     *
     * Case    = Case(pat: Pattern, body: Term')
     *
     * SelP    = Ref(sym: TermSymbol)
     *         | SelP(target: SelP, member: TermSymbol)
     *
     * Pattern = Wildcard
     *         | Lit[A](v: A)
     *         | SelP
     *         | AppP(target: SelP, targs: Seq[Type], argss: Seq[Seq[Pattern]])
     *         | TypedP(expr: Pattern, type: Type)
     *         | Bind(lhs: TermSymbol, rhs: Pattern)
     *
     * }}}
     */
    object Language {
      //@formatter:off

      // -----------------------------------------------------------------------
      // atomics
      // -----------------------------------------------------------------------

      val this_   = Term.this_          // this references
      val lit     = Term.lit            // literals
      val ref     = Term.ref            // references

      // -----------------------------------------------------------------------
      // terms
      // -----------------------------------------------------------------------

      val sel     = Term.sel            // selections
      val app     = Term.app            // function applications
      val inst    = Term.inst           // class instantiation
      val lambda  = Term.lambda         // lambdas
      val typed   = Type.ascription     // type ascriptions

      // -----------------------------------------------------------------------
      // state
      // -----------------------------------------------------------------------

      val val_    = Tree.val_           // val and var definitions
      val assign  = Tree.assign         // assignments
      val block   = Tree.block          // blocks

      // -----------------------------------------------------------------------
      // control flow
      // -----------------------------------------------------------------------

      val branch  = Tree.branch         // conditionals
      val whiledo = Tree.while_         // while loop
      val dowhile = Tree.doWhile        // do while loop

      // -----------------------------------------------------------------------
      // pattern matching
      // -----------------------------------------------------------------------

      val match_  = Tree.match_         // pattern match
      val case_   = Tree.case_          // case
      val bind    = Tree.bind           // bind

      //@formatter:on
    }

    // -------------------------------------------------------------------------
    // Validate API
    // -------------------------------------------------------------------------

    /** Delegates to [[SourceValidate.valid]]. */
    val valid = SourceValidate.valid

    /** Delegates to [[SourceValidate.valid]]. */
    def validate(tree: Tree): Boolean =
      valid(tree).isGood
  }

}
