package eu.stratosphere.emma.macros.program.controlflow

import eu.stratosphere.emma.macros.BlackBoxUtil
import eu.stratosphere.emma.util.Counter
import scala.annotation.tailrec
import scala.collection.mutable

private[emma] trait ControlFlowNormalization extends BlackBoxUtil {
  import universe._
  import c.internal._
  import syntax._

  /**
   * Normalize the control flow of a [[Tree]].
   * 
   * @param tree the [[Tree]] to be normalized
   * @return A copy if the [[Tree]] with normalized control flow
   */
  def normalize(tree: Tree): Tree = q"""{
    import _root_.scala.reflect._
    ${ tree                     ->>
       normalizeEnclosingParams ->>
       normalizeClassNames      ->>
       normalizeControlFlow }
  }""".typeChecked.as[Block].expr

  // --------------------------------------------------------------------------
  // Normalize control flow condition tests.
  // --------------------------------------------------------------------------

  /** 
   * Normalizes all [[Boolean]] predicate tests in an expression [[Tree]].
   *
   * This process includes:
   *
   * - un-nesting of complex [[Tree]]s outside while loop tests
   * - un-nesting of complex [[Tree]]s outside do-while loop tests
   * - un-nesting of complex [[Tree]]s outside if-then-else conditionals
   */
  object normalizeControlFlow extends (Tree => Tree) {
    val testCounter = new Counter()

    def apply(tree: Tree) = transform(tree) {
      case q"while (${cond: Tree}) $body"
        if (cond match { case _: Ident => false; case _ => true }) =>
          // Introduce condition variable
          val condVar = $"testWhile"
          // Move the complex test outside the condition
          q"{ var $condVar = $cond; while ($condVar) { $body; $condVar = $cond } }"

      case q"do $body while (${cond: Tree})"
        if (cond match { case _: Ident => false; case _ => true }) =>
          // Introduce condition variable
          val condVar = $"testDoWhile"
          // Move the complex test outside the condition
          q"""{
            var $condVar = null.asInstanceOf[Boolean]
            do { $body; $condVar = $cond } while ($condVar)
          }"""

      case q"if (${cond: Tree}) $thn else $els"
        if (cond match { case _: Ident => false; case _ => true }) =>
          // Introduce condition value
          val condVal = $"testIf"
          // Move the complex test outside the condition
          q"val $condVal = $cond; if ($condVal) $thn else $els"
    }
  }

  // --------------------------------------------------------------------------
  // Normalize enclosing object parameter access.
  // --------------------------------------------------------------------------

  private class EnclosingParamNormalizer(val clazz: Symbol) {
    val aliases = mutable.Map.empty[Symbol, ValDef]

    def normalize(tree: Tree): Tree = transform(tree) {
      case select @ Select(enclosing: This, name) if needsSubstitution(enclosing, select) =>
        val alias = aliases.getOrElseUpdate(select.symbol,
          val_(s"__this$$$name", select.preciseType) := select)

        &(alias.term)
    }

    /**
     * Check if a [[Select]] from an enclosing 'this' needs to be substituted.
     *
     * @param enclosing The enclosing `this` reference
     * @param select The referenced field to normalize
     * @return `true` if the `this` reference should be substituted
     */
    def needsSubstitution(enclosing: This, select: Select): Boolean =
      // Enclosing 'this' is a class, term selection, getter or val select
      enclosing.symbol == clazz && select.hasTerm &&
        (select.term.isStable || select.term.isGetter)
  }

  /**
   * Normalizes enclosing object parameter access.
   *
   * - Identifies usages of enclosing object parameters.
   * - Replaces the [[Select]]s with values of the form `__this${name}`.
   * - Assigns the accessed parameters to local values of the form `__this${name}` at the beginning
   *   of the code.
   */
  object normalizeEnclosingParams extends (Tree => Tree) {

    def apply(root: Tree) = findOwnerClass(enclosingOwner) match {
      case Some(clazz) =>
        // Construct function
        val epn = new EnclosingParamNormalizer(clazz)
        // Normalize tree and collect alias symbols
        val norm = epn.normalize(root)
        // Construct normalized code snippet
        q"{ ..${epn.aliases.values}; ..$norm }"

      case None => root
    }

    /**
     * Maybe return the first class owner of the [[Symbol]].
     *
     * @param sym The [[Symbol]] to test
     * @return The first class owner of the [[Symbol]], if any
     */
    @tailrec private def findOwnerClass(sym: Symbol): Option[Symbol] =
      if (sym == null || sym == NoSymbol) None
      else if (sym.isClass) Some(sym)
      else findOwnerClass(sym.owner)
  }

  // --------------------------------------------------------------------------
  // Normalize class names
  // --------------------------------------------------------------------------

  /** Substitutes the names of local classes to fully qualified names. */
  def normalizeClassNames(tree: Tree): Tree = transform(tree) {
    case id: Ident if id.hasSymbol && id.symbol.isModule => mk.select(id.symbol)
    case q"new ${id: Ident}[..$types](..${args: List[Tree]})" if id.hasSymbol =>
      q"new ${mk.typeSelect(id.symbol)}[..$types](..${args map normalizeClassNames})"
  }
}
