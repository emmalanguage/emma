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

    /**
     * Convert a tree into administrative normal form.
     *
     * == Preconditions ==
     *
     * - The input does not contain control flow or function definitions.
     * - There are no name clashes (can be ensured with `resolveNameClashes`).
     *
     * == Postconditions ==
     *
     * - Introduces dedicated symbols for chains of length greater than one.
     * - Ensures that all function arguments are trivial identifiers.
     *
     * @param tree The [[Tree]] to be converted.
     * @return An ANF version of the input [[Tree]].
     */
    // FIXME: What happens if there are methods with by-name parameters?
    def anf(tree: Tree): Tree = {
      assert(nameClashes(tree).isEmpty)
      assert(controlFlowNodes(tree).isEmpty)

      val anfTransform: Tree => Tree = postWalk {
        // Already in ANF
        case EmptyTree => EmptyTree
        case lit: Literal => block(lit)
        case id: Ident if id.isTerm => block(id)
        case value: ValDef if Is param value => value

        case fun: Function =>
          val x = fresh(nameOf(fun))
          val T = Type of fun
          val lhs = Term.sym.free(x, T)
          block(val_(lhs, fun), Term ref lhs)

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
          if (x.isPackage || // Parameter lists follow
            (x.isMethod && x.asMethod.paramLists.nonEmpty) ||
            IR.comprehensionOps.contains(x)) {

            block(stats, rhs)
          } else {
            val lhs = Term.sym.free(fresh(member), T)
            block(stats, val_(lhs, rhs), Term ref lhs)
          }

        case TypeApply(Block(stats, target), types) =>
          val expr = Type.app(target, types map Type.of: _*)
          block(stats, expr)

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
      }

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
     * - Except the nested blocks, the input tree is in simplified ANF form (see [[anf()]] and [[simplify()]]).
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

    /** Extracts control flow nodes from the given `tree`. */
    private def controlFlowNodes(tree: Tree): List[Tree] = tree collect {
      case branch: If => branch
      case patMat: Match => patMat
      case dd: DefDef => dd
      case loop: LabelDef => loop
    }

    /** Returns the set of [[Term]]s in `tree` that have clashing names. */
    private def nameClashes(tree: Tree): Seq[TermSymbol] =
      defs(tree).groupBy(_.name)
        .filter { case (_, defs) => defs.size > 1 }
        .flatMap { case (_, defs) => defs }.toSeq

    /**
     * Returns the encoded name associated with this subtree.
     */
    private def nameOf(tree: Tree): String = {

      @tailrec
      def loop(tree: Tree): Name = tree match {
        case id: Ident => id.name
        case value: ValDef => value.name
        case method: DefDef => method.name
        case Select(_, member) => member
        case Typed(expr, _) => loop(expr)
        case Block(_, expr) => loop(expr)
        case Apply(target, _) => loop(target)
        case TypeApply(target, _) => loop(target)
        case _: Function => Term.name.lambda
        case _: Literal => Term.name("x")
        case _ => throw new RuntimeException("Unsupported tree")
      }

      loop(tree).encodedName.toString
    }
  }

}
