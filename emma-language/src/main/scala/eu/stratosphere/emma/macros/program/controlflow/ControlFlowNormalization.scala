package eu.stratosphere.emma.macros.program.controlflow

import eu.stratosphere.emma.macros.BlackBoxUtil
import eu.stratosphere.emma.util.Counter
import scala.annotation.tailrec
import scala.collection.mutable

private[emma] trait ControlFlowNormalization extends BlackBoxUtil {
  import universe._
  import c.internal._

  /**
   * Normalize the control flow of a [[Tree]].
   * 
   * @param tree the [[Tree]] to be normalized
   * @return A copy if the [[Tree]] with normalized control flow
   */
  def normalize(tree: Tree): Tree = q"""{
    import _root_.scala.reflect._
    ${tree ->> EnclosingParamNormalizer ->> unTypeCheck ->> ControlFlowTestNormalizer}
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
  object ControlFlowTestNormalizer extends Transformer with (Tree => Tree) {
    val testCounter = new Counter()

    override def transform(tree: Tree) = tree match {
      // while (`cond`) { `body` }
      case LabelDef(_, _, If(cond, Block(body, _), _)) =>
        cond match {
          case _: Ident =>
            // If condition is a simple identifier, no normalization needed
            super.transform(tree)
          case _ =>
            // Introduce condition variable
            val condVar = TermName(f"testA${testCounter.advance.get}%03d")
            // Move the complex test outside the condition
            q"{ var $condVar = $cond; while ($condVar) { $body; $condVar = $cond } }"
        }

      // do { `body` } while (`cond`)
      case LabelDef(_, _, Block(body, If(cond, _, _))) =>
        cond match {
          case _: Ident =>
            // If condition is a simple identifier, no normalization needed
            super.transform(tree)
          case _ =>
            // Introduce condition variable
            val condVar = TermName(f"testB${testCounter.advance.get}%03d")
            // Move the complex test outside the condition
            q"""{
              var $condVar = null.asInstanceOf[Boolean]
              do { $body; $condVar = $cond } while ($condVar)
            }"""
        }

      // if (`cond`) `thn` else `els`
      case If(cond, thn, els) =>
        cond match {
          case _: Ident =>
            // If condition is a simple identifier, no normalization needed
            super.transform(tree)
          case _ =>
            // Introduce condition value
            val condVal = TermName(f"testC${testCounter.advance.get}%03d")
            // Move the complex test outside the condition
            q"val $condVal = $cond; if ($condVal) $thn else $els"
        }

      // Default case
      case _ => super.transform(tree)
    }

    def apply(tree: Tree) = transform(tree)
  }

  // --------------------------------------------------------------------------
  // Normalize enclosing object parameter access.
  // --------------------------------------------------------------------------

  private class EnclosingParamNormalizer(val clazz: Symbol) extends Transformer {
    val aliases = mutable.Map.empty[Symbol, ValDef]

    override def transform(tree: Tree) = tree match {
      case sel @ Select(enclosing: This, name) if needsSubstitution(enclosing, sel) =>
        val alias = aliases.getOrElseUpdate(sel.symbol,
          mk.valDef(TermName(s"__this$$$name"), sel.trueType, rhs = sel))

        Ident(alias.name)

      case _ => super.transform(tree)
    }

    /**
     * Check if a [[Select]] from an enclosing 'this' needs to be substituted.
     *
     * @param enclosing The enclosing `this` reference
     * @param select The referenced field to normalize
     * @return `true` if the `this` reference should be substituted
     */
    def needsSubstitution(enclosing: This, select: Select): Boolean =
      enclosing.symbol == clazz &&                     // Enclosing 'this' is a class
        select.hasTerm &&                              // Term selection
        (select.term.isStable || select.term.isGetter) // Getter or val select
  }

  /**
   * Normalizes enclosing object parameter access.
   *
   * - Identifies usages of enclosing object parameters.
   * - Replaces the [[Select]]s with values of the form `__this${name}`.
   * - Assigns the accessed parameters to local values of the form `__this${name}` at the beginning
   *   of the code.
   */
  object EnclosingParamNormalizer extends (Tree => Tree) {

    def apply(root: Tree) = findOwnerClass(enclosingOwner) match {
      case Some(clazz) =>
        // Construct function
        val normalize = new EnclosingParamNormalizer(clazz)
        // Normalize tree and collect alias symbols
        val normTree  = normalize transform root
        // Construct normalized code snippet
        q"{ ..${normalize.aliases.values}; ..$normTree }"

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
  object ClassNameNormalizer extends Transformer with (Tree => Tree) {

    override def transform(tree: Tree): Tree = tree match {
      // Search for a class call without 'new'
      case Apply(Select(id: Ident, name), args) if !isParam(id.symbol) =>
        val selChain = mk.select(id.symbol, apply = true)
        Apply(Select(selChain, name), args map transform)

      // Search for a class call with 'new'
      case Apply(Select(New(id: Ident), termNames.CONSTRUCTOR), args)
        if id.hasSymbol && !id.symbol.owner.isTerm =>
          val selChain = mk.select(id.symbol, apply = false)
          Apply(Select(New(selChain), termNames.CONSTRUCTOR), args map transform)

      case _ => super.transform(tree)
    }

    def apply(root: Tree) = transform(root)

    /**
     * Check if whether a [[Symbol]] is a parameter, function, `val` or a `var`.
     *
     * @param sym The [[Symbol]] to check
     * @return `true` if the [[Symbol]] is any of the above
     */
    def isParam(sym: Symbol): Boolean =
      (sym.isTerm && (sym.asTerm.isVal || sym.asTerm.isVar)) ||
        (sym.owner.isMethod && sym.isParameter)
  }
}
