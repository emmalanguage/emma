package eu.stratosphere.emma.compiler.lang.core

import eu.stratosphere.emma.compiler.Common
import eu.stratosphere.emma.compiler.lang.comprehension.Comprehension

/** Core language. */
trait Core extends Common
  with LNF
  with DCE
  with CSE
  with PatternMatching
  with CoreValidate
  with Comprehension {

  import universe._

  object Core {

    // -------------------------------------------------------------------------
    // Validate API
    // -------------------------------------------------------------------------

    /** Delegates to [[CoreValidate.validate()]]. */
    def validate(tree: Tree): Boolean =
      CoreValidate.validate(tree)

    // -------------------------------------------------------------------------
    // LNF API
    // -------------------------------------------------------------------------

    /** Delegates to [[LNF.lift()]]. */
    def lift(tree: Tree): Tree =
      LNF.lift(tree)

    /** Delegates to [[LNF.lower()]]. */
    def lower(tree: Tree): Tree =
      LNF.lift(tree)

    /** Delegates to [[LNF.resolveNameClashes()]]. */
    def resolveNameClashes(tree: Tree): Tree =
      LNF.resolveNameClashes(tree)

    /** Delegates to [[LNF.anf()]]. */
    def anf(tree: Tree): Tree =
      LNF.anf(tree)

    /** Delegates to [[LNF.flatten()]]. */
    def flatten(tree: Tree): Tree =
      LNF.flatten(tree)

    /** Delegates to [[LNF.simplify()]]. */
    def simplify(tree: Tree): Tree =
      LNF.simplify(tree)

    // -------------------------------------------------------------------------
    // DCE API
    // -------------------------------------------------------------------------

    /** Delegates to [[DCE.dce()]]. */
    def dce(tree: Tree): Tree =
      DCE.dce(tree)

    // -------------------------------------------------------------------------
    // CSE API
    // -------------------------------------------------------------------------

    /** Delegates to [[DCE.dce()]]. */
    def cse(tree: Tree): Tree =
      CSE.cse(tree)

    // -------------------------------------------------------------------------
    // PatternMatching API
    // -------------------------------------------------------------------------

    /** Delegates to [[PatternMatching.destructPatternMatches]]. */
    val destructPatternMatches: Tree => Tree =
      PatternMatching.destructPatternMatches

    // -------------------------------------------------------------------------
    // Meta Information API
    // -------------------------------------------------------------------------

    /**
     * Provides commonly used meta-information for an input [[Tree]].
     *
     * == Assumptions ==
     * - The input [[Tree]] is in LNF form.
     */
    class Meta(tree: Tree) {

      val defs: Map[Symbol, ValDef] = tree.collect {
        case value: ValDef if !Is.param(value) =>
          value.symbol -> value
      }.toMap

      val uses: Map[Symbol, Int] =
        tree.collect { case id: Ident => id.symbol }
          .view.groupBy(identity)
          .mapValues(_.size)
          .withDefaultValue(0)

      @inline
      def valdef(sym: Symbol): Option[ValDef] =
        defs.get(sym)

      @inline
      def valuses(sym: Symbol): Int =
        uses(sym)
    }

  }

}
