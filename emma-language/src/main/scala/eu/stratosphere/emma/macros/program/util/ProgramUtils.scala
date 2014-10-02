package eu.stratosphere.emma.macros.program.util

import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.comprehension.ComprehensionModel
import eu.stratosphere.emma.api.DataBag

import scala.collection.mutable.ListBuffer
import scala.reflect.macros.blackbox

private[emma] trait ProgramUtils[C <: blackbox.Context] extends ContextHolder[C] with ComprehensionModel[C] {

  import c.universe._

  val collTypeSymbols = List[Symbol](
    typeOf[DataBag[_]].typeSymbol
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
   * Simultaneously substitutes all occurrences of `x` with `y` in the given context `t` and adapts its environment.
   *
   * @param t The context to be modified
   * @param x The old node to be substituted
   * @param y The new node to substituted in place of the old
   * @return
   */
  def substitute(t: ScalaExpr, x: TermName, y: ScalaExpr) = {
    t.env = t.env.filter(_.name != x) ++ y.env.filterNot(t.env.contains(_))
    t.tree = new TermSubstituter(x.toString, y.tree).transform(t.tree)
  }

  /**
   * Simultaneously substitutes all occurrences of `x` with `y` in the given context `t`.
   *
   * @param t The term tree to be modified
   * @param x The identifier to be substituted
   * @param y The new term tree to be substituted in place of 'x'
   * @return
   */
  def substitute(t: Tree, x: String, y: Tree) = {
    new TermSubstituter(x, y).transform(t)
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
        // Monads
        case z@MonadUnit(expr) => z.expr = substitute(expr, x, y).asInstanceOf[MonadExpression]; z
        case z@MonadJoin(expr) => z.expr = substitute(expr, x, y).asInstanceOf[MonadExpression]; z
        case z@Comprehension(tpe, head, qualifiers) => z.head = substitute(head, x, y); z.qualifiers = for (q <- qualifiers) yield substitute(q, x, y).asInstanceOf[Qualifier]; z
        // Qualifiers
        case z@Filter(expr) => z.expr = substitute(expr, x, y); z
        case z@Generator(lhs, rhs) => z.rhs = substitute(rhs, x, y); z
        // Environment & Host Language Connectors
        case z@ScalaExpr(_, _) => z // FIXME
        case z@Read(_, _, _) =>  z
        case z@Write(_, _, _) =>  z
        // Logical Operators:
        case z@Group(_, _) => z
        case z@Fold(_, empty, sng, union, in) => z
        case z@Distinct(in) => z
        case z@Union(l, r) => z
        case z@Diff(l, r) => z
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

  private class TermSubstituter(val map: Map[String, Tree]) extends Transformer {

    def this(name: String, term: Tree) = this(Map[String, Tree](name -> term))

    override def transform(tree: Tree): Tree = tree match {
      case Ident(TermName(x)) => if (map.contains(x)) map(x) else tree
      case _ => super.transform(tree)
    }
  }

}
