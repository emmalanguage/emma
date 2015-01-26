package eu.stratosphere.emma.macros.program.util

import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.comprehension.ComprehensionModel

import scala.reflect.macros.blackbox

private[emma] trait ProgramUtils[C <: blackbox.Context] extends ContextHolder[C] with ComprehensionModel[C] {

  import c.universe._

  /**
   * Create a function that replaces all occurences of the identifiers from the given environment with fresh
   * identifiers. This can be used to "free" identifiers from their original symbols.
   *
   * @param vars An environment consisting of a list of ValDefs.
   * @return
   */
  def freeEnv(vars: List[Variable]) = new Transformer with (Tree => Tree) {
    val varsmap = (for (v <- vars) yield v.name).toSet

    override def transform(tree: Tree): Tree = tree match {
      case ident@Ident(name: TermName) =>
        if (varsmap.contains(name))
          Ident(name)
        else
          ident
      case _ =>
        super.transform(tree)
    }

    /**
     * Free a `tree` from the symbols associated with a set of TermName identifiers.
     */
    override def apply(tree: Tree) = transform(tree)
  }

  /**
   * Filter all environment entries referenced at least once in a given given tree.
   *
   * @param vars An environment consisting of a list of Variable.
   * @return
   */
  def referencedVars(vars: List[Variable]) = new Traverser with (Tree => Set[TermName]) {
    val defsmap = (for (v <- vars) yield v.name).toSet
    var referenced = Set[TermName]()

    override def traverse(tree: Tree): Unit = tree match {
      case ident@Ident(name: TermName) => referenced = referenced + name
      case _ => super.traverse(tree)
    }

    /**
     * Check a term tree for the given set of `vars`.
     */
    override def apply(tree: Tree) = {
      traverse(tree)
      defsmap.filter(referenced.contains)
    }
  }

  /**
   * Simultaneously substitutes all occurrences of `x` with `y` in the given context `t` and adapts its environment.
   *
   * @param t The context to be modified
   * @param x The old node to be substituted
   * @param y The new node to substituted in place of the old
   * @return
   */
  def substitute(t: ScalaExpr, x: TermName, y: ScalaExpr) = {
    // remove x from the vars visible to t
    t.vars = t.vars.filter(_.name != x)
    // add all additional y.vars to t.vars
    t.vars = t.vars ++ y.vars.filterNot(t.vars.contains(_))
    t.tree = c.typecheck(new TermSubstituter(x.toString, y.tree).transform(t.tree))
  }

  /**
   * Simultaneously substitutes all occurrences of `x` with `y` in the given context `t`.
   *
   * @param t The term tree to be modified
   * @param x The identifier to be substituted
   * @param y The new term tree to be substituted in place of 'x'
   * @return
   */
  def substitute(t: Tree, x: TermName, y: Tree) = {
    new TermSubstituter(x.toString, y).transform(t)
  }

  /**
   * Simultaneously substitutes all occurrences of `x` with `y` in the given context `t`.
   *
   * @param t The term tree to be modified
   * @param map A map of `x` identifiers and their corresponding `y` substitutions.
   * @return
   */
  def substitute(t: Tree, map: Map[String, Tree]) = {
    new TermSubstituter(map).transform(t)
  }

  /**
   * Inlines the right-hand side of the given `valdef` into the given context tree `t` and removes it from `t`.
   *
   * @param t The context tree to be rewritten.
   * @param valdef The value definition to be inlined
   * @return A version of the context tree with the value definition inlined
   */
  def inline(t: Tree, valdef: ValDef) = {
    new ValDefInliner(valdef).transform(t)
  }

  // ---------------------------------------------------
  // Code traversers.
  // ---------------------------------------------------

  private class TermSubstituter(val map: Map[String, Tree]) extends Transformer {

    def this(name: String, term: Tree) = this(Map[String, Tree](name -> term))

    override def transform(tree: Tree): Tree = tree match {
      case Ident(TermName(x)) => if (map.contains(x)) map(x) else tree
      case _ => super.transform(tree)
    }
  }

  private class ValDefInliner(val valdef: ValDef) extends Transformer {

    override def transform(tree: Tree): Tree = tree match {
      case t@ValDef(_, _, _, _) if t.symbol == valdef.symbol => EmptyTree
      case t@Ident(TermName(x)) if t.symbol == valdef.symbol => valdef.rhs
      case _ => super.transform(tree)
    }
  }

}
