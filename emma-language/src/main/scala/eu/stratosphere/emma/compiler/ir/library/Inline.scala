package eu.stratosphere.emma.compiler.ir.library

import eu.stratosphere.emma.api.Inlined
import eu.stratosphere.emma.compiler.RuntimeCompiler
import scala.annotation.tailrec

/** Library behavior for the [[RuntimeCompiler]] */
trait Inline { this: RuntimeCompiler =>
  import universe._
  lazy val mirror: universe.Mirror = tb.mirror

  private[emma] val INLINE_FIXED_POINT_LIMIT = 100

  /** Very simple memoization for [[Function1]] i.e. [[(A => R]] functions.
    *
    * TODO brain: Currently implemented using [[collection.mutable.WeakHashMap]],
    * which may not be ideal.
    *
    * @param f the function to be memoized
    * @tparam A argument type (contravariant)
    * @tparam R result type (covariant)
    */
  class Memo[-A, +R](f: A => R) extends (A => R) {
    private[this] val memo = collection.mutable.WeakHashMap.empty[A, R]
    def apply(a: A): R = memo.getOrElseUpdate(a, f(a))
  }

  /** Memo for [[mirror.reflectModule]] */
  private val reflectModule = new Memo[ModuleSymbol, ModuleMirror](mirror.reflectModule)

  /** Memo for [[mirror.reflect]] */
  private val reflect = new Memo[Any, InstanceMirror](mirror.reflect)

  /** Performs beta-reduction on a tree
    *
    * Example:
    *
    * {{{
    *   val lo = 23
    *   val hi = 66
    *   val between = ((lo: Int, hi: Int) => (x: Int) => lo <= x && x < hi)(lo, hi)(42)
    * }}}
    *
    * becomes
    *
    * {{{
    *   val lo = 23
    *   val hi = 66
    *   val between = lo <= 42 && 42 < hi
    * }}}
    *
    * @param t the tree
    * @return the beta-reduced tree
    */
  def betaReduce(t: Tree): Tree = {
    def bRed(body:Tree, params: List[ValDef], args: List[Tree]) = {
      assert(params.size == args.size)
      Tree.subst(body, (params.map(_.symbol) zip args).toMap)
    }
    postWalk(t) {
      case Apply(Function(params, body), args) =>
        bRed(body, params, args)
      case Apply(Select(Function(params, body), TermName("apply")), args) =>
        bRed(body, params, args)
    }
  }

  /** Inlines all applications to methods tagged with the [[Inlined]] runtime annotation.
    *
    * Example:
    *
    * Assuming there is a inlined method
    *
    * {{{
    *   @emma.inline object MyLibrary {
    *     def geq(lower: Double)(x: Double) = lower <= x
    *   }
    * }}}
    *
    * then an inlined application
    *
    * {{{
    *   val isGeq = MyLibrary.geq(23.0)(42.0)
    * }}}
    *
    * will result in:
    *
    * {{{
    *   val isGeq: Boolean = ((lower: Double) => ((x: Double) => lower.<=(x)))(23.0)(42.0);
    * }}}
    *
    * @param t the original tree
    * @return the resulting tree with one iteration of inlined methods.
    */
  private[emma] def transformApplications(t: Tree): Tree = postWalk(t) {
    case functionSelection @ Select(moduleSelection @ Select(qualifier, name), func)
      if qualifier.symbol.isModule && !qualifier.symbol.hasPackageFlag =>
      val qualifierMirror = reflectModule(qualifier.symbol.asModule)
      val modSymbol = qualifierMirror.symbol.info.member(name).asModule
      val modMirror = reflectModule(modSymbol)
      val modInstanceMirror = reflect(modMirror.instance)
      val modFunc = functionSelection.symbol.asMethod
      val maybeReplacement = for {
        ann <- Symbol.annotation[Inlined](modFunc)
        Literal(Constant(target)) <- findArg(ann, TermName("forwardTo"))
      } yield {
        val targetTermName = TermName(target.asInstanceOf[String])
        val targetMethodSymbol = modSymbol.info.decl(targetTermName).asMethod
        val targetMethodMirror = modInstanceMirror.reflectMethod(targetMethodSymbol)
        targetMethodMirror(universe) match {
          case e: Expr[_] => preWalk(Type.check(e.tree)) {
            case a@Apply(sel@Select(This(qualifierThis), funName), args) =>
              transformApplications(Method.call(functionSelection.qualifier, sel.symbol.asMethod)(List(args: _*)))
          }
          case x =>
            abort(functionSelection.pos, s"Unable to inline $functionSelection")
            functionSelection
        }
      }
      maybeReplacement.getOrElse(functionSelection)
  }

  /** Inlines all applications to methods tagged with the [[Inlined]] runtime annotation
    * until a fixed point is reached.
    *
    * @param tree
    * @return
    */
  def transformInline(tree: Tree): Tree = {
    @tailrec def _transformInline(tree: Tree, acc: Int = 0):Tree = transformApplications(tree) match {
      case newTree if tree equalsStructure newTree => newTree
      case newTree: Tree if acc >= INLINE_FIXED_POINT_LIMIT =>
        abort(tree.pos, s"Inlining did not reach a fixed point after $acc iterations for $tree")
        tree
      case newTree: Tree => _transformInline(newTree, acc + 1)
    }
    _transformInline(tree)
  }


  /** Runtime annotation helper, extracts a list of [[AssignOrNamedArg]]s from
    * a given [[Tree]].
    *
    * @return a list of annotation arguments as [[AssignOrNamedArg]]
    */
  private[emma] def extractArgs: Tree => List[AssignOrNamedArg] = {
    case Apply(Select(New(TypeTree()), termNames.CONSTRUCTOR), lst: List[_]) =>
      lst.asInstanceOf[List[AssignOrNamedArg]]
    case _ => List.empty
  }

  /** Returns the right-hand-side of an annotation argument if it exists.
    *
    * @param a the annotation
    * @param tn the [[TermName]]
    * @return [[Some]] [[Tree]] if the [[TermName]] was found, [[None]] else
    */
  private[emma] def findArg(a: Annotation, tn: TermName): Option[Tree] = extractArgs(a.tree).collectFirst {
    case AssignOrNamedArg(Ident(`tn`), rhs) => rhs
  }
}
