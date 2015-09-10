package eu.stratosphere.emma.macros.program.util

import eu.stratosphere.emma.macros.program.comprehension.ComprehensionModel

private[emma] trait ProgramUtils extends ComprehensionModel {
  import universe._

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
}
