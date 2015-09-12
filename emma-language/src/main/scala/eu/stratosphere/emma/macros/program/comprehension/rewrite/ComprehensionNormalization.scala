package eu.stratosphere.emma.macros.program.comprehension.rewrite

import scala.util.matching.Regex

trait ComprehensionNormalization extends ComprehensionRewriteEngine {
  import universe._

  def normalize(root: ExpressionRoot) = {
    applyExhaustively(UnnestHead, UnnestGenerator, SimplifyTupleProjection)(root)
    applyExhaustively(FoldFusion)(root)
    root
  }

  /**
   * Un-nests a comprehended head in its parent.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ e | qs, x ← [[ e' | qs' ]], qs'' ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e[e'\x] | qs,  qs', qs''[e'\x] ]] }}}
   */
  object UnnestGenerator extends Rule {

    case class RuleMatch(parent: Comprehension, gen: Generator, child: Comprehension)

    def bind(expr: Expression) = expr match {
      case parent @ Comprehension(_, qualifiers) => qualifiers collectFirst {
        case gen @ Generator(_, child @ Comprehension(_: ScalaExpr, _)) =>
          RuleMatch(parent, gen, child)
      }
      
      case _ => None
    }

    def guard(rm: RuleMatch) = true

    def fire(rm: RuleMatch) = {
      val RuleMatch(parent, gen, child) = rm
      val rest = parent
        .dropWhile { _ != gen }.tail // trim prefix
        .takeWhile { expr => !expr.isInstanceOf[Generator] ||
          expr.as[Generator].lhs.fullName != gen.toString
        } // trim suffix

      val (xs, ys) = parent.qualifiers span { _ != gen }
      parent.qualifiers = xs ::: child.qualifiers ::: ys.tail

      for (expr <- rest if expr.isInstanceOf[ScalaExpr])
        expr.as[ScalaExpr].substitute(gen.lhs.name, child.hd.as[ScalaExpr])

      // new parent
      rm.parent
    }
  }

  /**
   * Unnests a comprehended head in its parent.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ join [[ [[ e | qs' ]] | qs ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e | qs, qs' ]] }}}
   */
  object UnnestHead extends Rule {

    case class RuleMatch(parent: Comprehension, child: Comprehension)

    def bind(expr: Expression) = expr match {
      case MonadJoin(parent @ Comprehension(child: Comprehension, _)) =>
        Some(RuleMatch(parent, child))
      
      case _ => None
    }

    def guard(rm: RuleMatch) = true //FIXME

    def fire(rm: RuleMatch) = {
      val RuleMatch(parent, child) = rm
      parent.qualifiers ++= child.qualifiers
      parent.hd = child.hd
      parent // return new root
    }
  }

  object SimplifyTupleProjection extends Rule {

    val regex = "_(\\d{1,2})".r

    case class RuleMatch(expr: ScalaExpr)

    def bind(expr: Expression) = expr match {
      case expr: ScalaExpr => Some(RuleMatch(expr))
      case _ => None
    }

    def guard(rm: RuleMatch) =
      new PatternMatcher(regex) apply rm.expr.tree

    def fire(rm: RuleMatch) = {
      rm.expr.tree = new Simplifier(regex) apply rm.expr.tree
      rm.expr // return new root
    }

    class PatternMatcher(pattern: Regex) extends Traverser with (Tree => Boolean) {

      var result = false

      override def traverse(tree: Tree) = tree match {
        case Select(Apply(TypeApply(Select(tuple @ Select(Ident(TermName("scala")), _), TermName("apply")), _), _), _: TermName)
          if isProjection(tree, tuple, pattern) => result = true
          
        case _ => super.traverse(tree)
      }

      def apply(tree: Tree) = {
        result = false
        traverse(tree)
        result
      }
    }

    class Simplifier(pattern: Regex) extends Transformer with (Tree => Tree) {

      var result = false

      override def transform(tree: Tree) = tree match {
        case Select(Apply(TypeApply(Select(tuple @ Select(Ident(TermName("scala")), _), TermName("apply")), types), args), _: TermName)
          if isProjection(tree, tuple, pattern) =>
            val offset = pattern.findFirstMatchIn(tree.symbol.fullName).get.group(1).toInt
            if (args.size >= offset && types.size >= offset) q"${args(offset - 1)}"
            else super.transform(tree)

        case _ => super.transform(tree)
      }

      def apply(tree: Tree) =
        transform(tree)
    }

    private def isProjection(tree: Tree, tuple: Select, pattern: Regex) =
      tuple.name.toString.startsWith("Tuple") &&
      tuple.hasSymbol && tuple.symbol.isModule &&
      tuple.symbol.companion.asClass.baseClasses.contains(symbolOf[Product]) &&
      tree.hasSymbol && tree.symbol.isMethod &&
      pattern.findFirstIn(tree.symbol.fullName).isDefined
  }

  /**
   * Fuses a fold with a child comprehension consisting of a single generator.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ fold( empty, sng, union, [[ e | x ← e' ]] ) }}}
   *
   * '''Rewrite''':
   * {{{ fold( empty, sng[e[x\z]\x], union[e[x\z]\x, e[y\z]\y], e' ]] ) }}}
   */
  object FoldFusion extends Rule {
    val c = combinator

    case class RuleMatch(fold: c.Fold, map: Comprehension, child: Generator)

    def bind(expr: Expression) = expr match {
      case fold @ c.Fold(_, _, _, map @ Comprehension(_: ScalaExpr, List(child: Generator)), _) =>
        Some(RuleMatch(fold, map, child))
      
      case _ => None
    }

    def guard(rm: RuleMatch) = true //FIXME

    def fire(rm: RuleMatch) = {
      val RuleMatch(fold, map, child) = rm
      val x    = freshName("x$")
      val head = map.hd.as[ScalaExpr].tree.rename(child.lhs.name, x)
      val sng  = fold.sng.as[Function]
      val body = sng.body.substitute(sng.vparams.head.name, head)
      fold.sng = q"($x: ${child.tpe}) => $body".typeChecked
      fold.xs  = child.rhs
      fold // return new root
    }
  }
}
