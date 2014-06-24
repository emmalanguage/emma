package eu.stratosphere.emma.macros.program.util

import _root_.eu.stratosphere.emma.macros.program.ContextHolder
import _root_.eu.stratosphere.emma.macros.program.ir.IntermediateRepresentation

import _root_.scala.collection.mutable.ListBuffer
import _root_.scala.reflect.macros.blackbox.Context

trait ProgramUtils[C <: Context] extends ContextHolder[C] with IntermediateRepresentation[C] {

  import c.universe._

  val collTypeSymbols = List[Symbol](
    typeOf[eu.stratosphere.emma.DataBag[_]].typeSymbol,
    typeOf[eu.stratosphere.emma.DataSet[_]].typeSymbol
  )

  /**
   * Checks whether a type symbol belongs to one of the supported collection types.
   *
   * @param typeSymbol The type sumbol to be checked.
   * @return
   */
  def isCollectionType(typeSymbol: Symbol) = collTypeSymbols.contains(typeSymbol)

  /**
   * Resolves the definition of an identifier through a ValDef node within the given scope. Returns the right-hand side
   * of the corresponding ValDef definition if tree is an identifier, or the tree itself otherwise.
   *
   * @param scope The scope within the tree definition is resolved
   * @param tree The (potential) identifier to be resolved
   * @return
   */
  def resolve(scope: Tree)(tree: Tree) = tree match {
    case Ident(n: TermName) => findValDef(scope)(n).getOrElse(c.abort(scope.pos, "Could not find definition of val '" + n + "' within scope")).rhs
    case _ => tree
  }

  /**
   * Extracts the symbols of the sinks returned by an Emma program.
   *
   * @param e The return statement of an Emma program.
   * @return
   */
  def extractSinkExprs(e: Tree): Set[Tree] = e match {
    case Apply(TypeApply(Select(Select(Ident(scala), x), TermName("apply")), _), args) =>
      if (x.toString.substring(0, x.toString.length - 1) != "Tuple") {
        c.abort(e.pos, "Emma programs must end with a tuple of sink identifiers")
      }
      args.toSet
    case _ =>
      Set(e)
  }

  /**
   * Find the ValDef scope for the given TermName.
   *
   * @param scope The enclosing search scope.
   * @param name The ValDef to be looked up.
   * @return
   */
  def findValDef(scope: Tree)(name: TermName): Option[ValDef] = {
    scope.find {
      case ValDef(_, n, _, _) => n == name
      case _ => false
    }.asInstanceOf[Option[ValDef]]
  }

  /**
   * Replace all occurences of the identifiers from the given environment with a fresh identifiers. This
   * can be used to "free" identifiers from their original symbols.
   *
   * @param tree The tree to be modified
   * @param env An environment consisting of a list of ValDefs.
   * @return
   */
  def freeEnv(tree: Tree, env: List[ValDef]) = {
    new IdentReplacer(env).transform(tree)
  }

  /**
   * Filter all environment entries referenced at least once in a given given tree.
   *
   * @param tree The tree to be checked
   * @param env An environment consisting of a list of ValDefs.
   * @return
   */
  def referencedEnv(tree: Tree, env: List[ValDef]) = {
    val checker = new IdentChecker(env)
    checker.traverse(tree)
    val result = env.filter(x => checker.referenced.contains(x.name))
    result
  }

  /**
   * Simultaneously substitutes all occurrences of `x` with `y` in the given context `t` and adapts its environment.
   *
   * @param t The context to be modified
   * @param x The old node to be substituted
   * @param y The new node to substituted in place of the old
   * @return
   */
  def substitute(t: ScalaExpr, x: String, y: ScalaExpr) = {
    t.env = t.env.filter(_.name.toString != x) ++ y.env.filterNot(t.env.contains(_))
    t.tree = new TermSubstituter(x, y.tree).transform(t.tree)
  }

  /**
   * Simultaneously substitutes all occurrences of a `x` with `y` in the given IR tree `t`.
   *
   * @param t The context to be modified
   * @param x The old node to be substituted
   * @param y The new node to substituted in place of the old
   * @return
   */
  def substitute(t: Expression, x: Expression, y: Expression): Expression =
    if (t == x)
      y
    else
      t match {
        case z@MonadUnit(expr) => z.expr = substitute(expr, x, y).asInstanceOf[MonadExpression]; z
        case z@MonadJoin(expr) => z.expr = substitute(expr, x, y).asInstanceOf[MonadExpression]; z
        case z@Filter(expr) => z.expr = substitute(expr, x, y); z
        case z@ScalaExprGenerator(lhs, rhs) => z.rhs = substitute(rhs, x, y).asInstanceOf[ScalaExpr]; z
        case z@ComprehensionGenerator(lhs, rhs) => z.rhs = substitute(rhs, x, y).asInstanceOf[MonadExpression]; z
        case z@ScalaExpr(_, _) => z
        case z@Comprehension(monad, head, qualifiers) => z.head = substitute(head, x, y); z.qualifiers = for (q <- qualifiers) yield substitute(q, x, y).asInstanceOf[Qualifier]; z
      }

  // ---------------------------------------------------
  // Code traversers.
  // ---------------------------------------------------

  private class DependencyTermExtractor(val scope: Tree, val term: TermTree) extends Traverser {
    val result = ListBuffer[(TermTree, Option[TermName])]()

    override def traverse(tree: Tree): Unit = {
      if (tree != term && tree.isTerm && isCollectionType(tree.tpe.typeSymbol)) {
        tree match {
          case ident@Ident(name: TermName) =>
            val deftree = findValDef(scope)(name).getOrElse(c.abort(scope.pos, "Could not find definition of val '" + name.toString + "' within this scope")).rhs.asInstanceOf[TermTree]
            result += Tuple2(deftree, Some(name))
          case _ => // TermTree
            result += Tuple2(tree.asInstanceOf[TermTree], None)
        }
      }
      super.traverse(tree)
    }
  }

  private class IdentReplacer(valdefs: List[ValDef]) extends Transformer {
    val defsmap = (for (v <- valdefs) yield v.name).toSet

    override def transform(tree: Tree): Tree = tree match {
      case ident@Ident(name: TermName) =>
        if (defsmap.contains(name))
          Ident(name)
        else
          ident
      case _ =>
        super.transform(tree)
    }
  }

  private class IdentChecker(valdefs: List[ValDef]) extends Traverser {
    val defsmap = (for (v <- valdefs) yield v.name).toSet
    var referenced = Set[TermName]()

    override def traverse(tree: Tree): Unit = tree match {
      case ident@Ident(name: TermName) => referenced = referenced + name
      case _ => super.traverse(tree)
    }
  }

  private class TermSubstituter(val name: String, val term: Tree) extends Transformer {
    override def transform(tree: Tree): Tree = tree match {
      case Ident(TermName(x)) => if (x == name) term else tree
      case _ => super.transform(tree)
    }
  }

}
