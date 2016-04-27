package eu.stratosphere.emma.compiler.lang.core

import eu.stratosphere.emma.compiler.Common

import scala.annotation.tailrec

/** Core language lifting / lowering. Uses Direct-style lef-normal form (LNF). */
private[core] trait LNF extends Common {
  self: Core =>

  import universe._
  import Term._
  import Tree._
  import Term.name.fresh

  private[core] object LNF {

    /**
     * Lift a Scala Source language [[Tree]] into let-normal form.
     *
     * This includes:
     *
     * - bringing the original tree to administrative normal form;
     * - modeling control flow as direct-style;
     * - inlining Emma API expressions.
     *
     * @param tree The core language [[Tree]] to be lifted.
     * @return A direct-style let-normal form variant of the input [[Tree]].
     */
    def lift(tree: Tree): Tree = {
      tree
    }

    /**
     * Lower a LNF [[Tree]] back as Emma core language [[Tree]].
     *
     * @param tree A direct-style let-nomal form [[Tree]].
     * @return A Scala [[Tree]] derived by deconstructing the IR tree.
     */
    def lower(tree: Tree): Tree = {
      tree
    }

    /** Ensures that all definitions within the `tree` have unique names. */
    def resolveNameClashes(tree: Tree): Tree =
      refresh(tree, nameClashes(tree): _*)

    private val anfTransform: Tree => Tree = postWalk {
      // Already in ANF
      case EmptyTree => EmptyTree
      case lit: Literal => block(lit)
      case ths: This => block(ths)
      case id: Ident if id.isTerm => block(id)
      case value: ValDef if Is param value => value

      case fun: Function =>
        val x = fresh(nameOf(fun))
        val T = Type of fun
        val lhs = Term.sym.free(x, T)
        block(val_(lhs, fun), Term ref lhs)

      case branch@If(Block(stats, cond), thn, els) =>
        val T = Type.of(branch)
        val x = fresh(nameOf(branch))
        val lhs = Term.sym.free(x, T)
        val rhs = Tree.branch(cond, expr(thn), expr(els))
        block(stats, val_(lhs, rhs), Term ref lhs)

      case Typed(Block(stats, expr), tpt) =>
        val x = fresh(nameOf(expr))
        val T = Type of tpt
        val lhs = Term.sym.free(x, T)
        val rhs = Type.ascription(expr, T)
        block(stats, val_(lhs, rhs), Term ref lhs)

      case sel@Select(Block(stats, target), _: TypeName) =>
        val x = Type sym sel
        val T = Type of sel
        block(stats, Type.sel(target, x, T))

      case sel@Select(Block(stats, target), member: TermName) =>
        val x = Term sym sel
        val T = Type of sel
        val rhs = Term.sel(target, x, T)
        if (x.isPackage || Is.method(T) || IR.comprehensionOps.contains(x)) {
          block(stats, rhs)
        } else {
          val lhs = Term.sym.free(fresh(member), T)
          block(stats, val_(lhs, rhs), Term ref lhs)
        }

      case TypeApply(Block(stats, target), types) =>
        val rhs = Term.app(target, types map Type.of: _*)()
        val T = Type of rhs
        if (Is.method(T) || IR.comprehensionOps.contains(Term sym target)) {
          block(stats, rhs)
        } else {
          val x = fresh(nameOf(target))
          val lhs = Term.sym.free(x, T)
          block(stats, val_(lhs, rhs), Term ref lhs)
        }

      case app@Apply(Block(stats, target), args) =>
        if (IR.comprehensionOps contains Term.sym(target)) {
          val expr = Term.app(target)(args)
          block(stats, expr)
        } else {
          val x = fresh(nameOf(target))
          val T = Type of app
          val lhs = Term.sym.free(x, T)
          val init = stats ::: args.flatMap {
            case Block(nested, _) => nested
            case _ => Nil
          }

          val params = args.map {
            case Block(_, expr) => expr
            case arg => arg
          }

          val rhs = Term.app(target)(params)
          // Partially applied multi-arg-list method
          if (Is method Type.of(app)) block(init, rhs)
          else block(init, val_(lhs, rhs), Term ref lhs)
        }

      // Only if contains nested blocks
      case block: Block if block.children.exists {
        case _: Block => true
        case _ => false
      } =>
        val body = block.children.flatMap {
          case nested: Block => nested.children
          case child => child :: Nil
        }

        // Implicitly removes ()
        Tree.block(body.init, body.last)

      // Avoid duplication of intermediates
      case val_(lhs, Block(stats :+ (int: ValDef), rhs: Ident), flags)
        if int.symbol == rhs.symbol =>
        block(stats, val_(lhs, int.rhs, flags), unit)

      case val_(lhs, Block(stats, rhs), flags) =>
        block(stats, val_(lhs, rhs, flags), unit)

      case Assign(Block(lstats, lhs), Block(rstats, rhs)) =>
        block(lstats ++ rstats, assign(lhs, rhs), unit)
    }

    /**
     * Converts a tree into administrative normal form.
     *
     * == Preconditions ==
     *
     * - There are no name clashes (can be ensured with `resolveNameClashes`).
     * - None of the following constructs are present:
     *     1. Pattern matching (can be ensured with `destructPatternMatches`)
     *     2. `try-catch-finally` exception handling
     *     3. Explicit `return` statements
     *
     * == Postconditions ==
     *
     * - Introduces dedicated symbols for chains of length greater than one.
     * - Ensures that all function arguments are trivial identifiers.
     * - The following constructs are bypassed (violate strict ANF):
     *     1. `if` branches
     *     2. `while` and `do-while` loops
     *     3. Local method `def`s
     *     4. Monadic `for` comprehensions
     *
     * @param tree The [[Tree]] to be converted.
     * @return An ANF version of the input [[Tree]].
     */
    // FIXME: What happens if there are methods with by-name parameters?
    def anf(tree: Tree): Tree = {

      assert(nameClashes(tree).isEmpty, s"Name clashes found in:\n${Tree show tree}")
      assert(tree forAll {
        case _: Match => false
        case _: Try => false
        case _: Return => false
        case _ => true
      }, s"Unsupported control flow nodes (see documentation):\n${Tree show tree}")
      anfTransform(tree)
    }

    /**
     * Inlines `Ident` return expressions in blocks whenever refered symbol is used only once.
     * The resulting [[Tree]] is said to be in ''simplified ANF'' form.
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[Core.anf()]]).
     *
     * == Postconditions ==
     * - `Ident` return expressions in blocks have been inlined whenever possible.
     *
     * @param tree The [[Tree]] to be normalized.
     * @return A [[Tree]] with the same semantics but without common subexpressions.
     */
    def simplify(tree: Tree): Tree = {

      def uses(sym: Symbol, stats: List[Tree]): Boolean = Block(stats, EmptyTree) exists {
        case id: Ident if id.symbol == sym => true
        case _ => false
      }

      def prune(sym: Symbol, stats: List[Tree]): (List[Tree], Option[Tree]) = stats
        .reverse
        .foldLeft((List.empty[Tree], Option.empty[Tree]))((res, tree) => tree match {
          case ValDef(_, _, _, rhs) if tree.symbol == sym =>
            res.copy(_2 = Some(rhs))
          case _ =>
            res.copy(_1 = tree :: res._1)
        })

      postWalk(tree) {
        case bl@Block(stats, expr: Ident) if !uses(expr.symbol, stats) =>
          val (pstats, pexpr) = prune(expr.symbol, stats)
          pexpr.map(expr => block(pstats, expr)).getOrElse(bl)
      }
    }

    /**
     * Unnests nested blocks [[Tree]].
     *
     * == Preconditions ==
     * - Except the nested blocks, the input tree is in simplified ANF form (see [[anf()]] and
     * [[simplify()]]).
     *
     * == Postconditions ==
     * - A simplified ANF tree where all nested blocks have been flattened.
     *
     * @param tree The [[Tree]] to be normalized.
     * @return A [[Tree]] with the same semantics but without common subexpressions.
     */
    def flatten(tree: Tree): Tree = {

      def hasNestedBlocks(tree: Block): Boolean = {
        val inStats = tree.stats exists {
          case ValDef(_, _, _, _: Block) => true
          case _ => false
        }
        val inExpr = tree.expr match {
          case _: Block => true
          case _ => false
        }
        inStats || inExpr
      }

      postWalk(tree) {
        case parent: Block if hasNestedBlocks(parent) =>
          // flatten (potentially) nested block stats
          val flatStats = parent.stats.flatMap {
            case val_(sym, Block(stats, expr), flags) =>
              stats :+ val_(sym, expr, flags)
            case stat =>
              stat :: Nil
          }
          // flatten (potentially) nested block expr
          val flatExpr = parent.expr match {
            case Block(stats, expr) =>
              stats :+ expr
            case expr =>
              expr :: Nil
          }

          block(flatStats ++ flatExpr)
      }
    }

    /** Returns the set of [[Term]]s in `tree` that have clashing names. */
    private def nameClashes(tree: Tree): Seq[TermSymbol] =
      defs(tree).groupBy(_.name)
        .filter { case (_, defs) => defs.size > 1 }
        .flatMap { case (_, defs) => defs }.toSeq

    /** Returns the encoded name associated with this subtree. */
    private def nameOf(tree: Tree): String = {
      @tailrec
      def loop(tree: Tree): Name = tree match {
        case id: Ident => id.name
        case branch: If => Term name s"if$$${nameOf(branch.cond)}"
        case value: ValDef => value.name
        case method: DefDef => method.name
        case loop: LabelDef => loop.name
        case sel: Select => sel.name
        case Typed(expr, _) => loop(expr)
        case Block(_, expr) => loop(expr)
        case Apply(target, _) => loop(target)
        case TypeApply(target, _) => loop(target)
        case _: Function => Term.name.lambda
        case _: Literal => Term name "x"
        case _ => throw new RuntimeException("Unsupported tree")
      }

      loop(tree).encodedName.toString
    }
  }

  /** Strips blocks with no statements, */
  private def expr(tree: Tree) = tree match {
    case Block(Nil, expr) => expr
    case _ => tree
  }
}
