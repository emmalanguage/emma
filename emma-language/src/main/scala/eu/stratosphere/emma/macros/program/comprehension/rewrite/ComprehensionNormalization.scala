package eu.stratosphere.emma.macros.program.comprehension.rewrite

import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.comprehension.ComprehensionModel
import eu.stratosphere.emma.macros.program.util.ProgramUtils
import eu.stratosphere.emma.rewrite.RewriteEngine

import scala.reflect.macros.blackbox

trait ComprehensionNormalization[C <: blackbox.Context]
  extends ContextHolder[C]
  with ComprehensionModel[C]
  with ProgramUtils[C]
  with RewriteEngine {

  import c.universe._

  val rules: List[Rule] = List()

  private val rulesV1: List[Rule] = List(SimplifyTupleProjection, UnnestHead, UnnestGenerator)

  private val rulesV2: List[Rule] = List(FuseFoldMap)

  override def rewrite(root: ExpressionRoot): ExpressionRoot = {
    while ((for (r <- rulesV1) yield r.apply(root)).fold(false)(_ || _)) {} // apply rules while possible
    while ((for (r <- rulesV2) yield r.apply(root)).fold(false)(_ || _)) {} // apply rules while possible
    root
  }

  def normalize(root: ExpressionRoot) = rewrite(root)

  /**
   * Unnests a comprehended head in its parent.
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

    case class RuleMatch(parent: Comprehension, generator: Generator, child: Comprehension)

    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
      override def foreach[U](f: (RuleMatch) => U) = {
        for (x <- globalSeq(r.expr)) x match {
          case parent@Comprehension(_, _, qualifiers) =>
            for (y <- qualifiers) y match {
              case generator@Generator(_, child@Comprehension(_, ScalaExpr(_, _), _)) =>
                f(RuleMatch(parent, generator, child))
              case _ => Unit
            }
          case _ => Unit
        }
      }
    }

    override protected def guard(r: ExpressionRoot, m: RuleMatch) = true

    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {
      val name = m.generator.lhs
      val term = m.child.head.asInstanceOf[ScalaExpr]
      val rest = localSeq(m.parent)
        .span(_ != m.generator)._2.tail // trim prefix
        .span(x => !x.isInstanceOf[Generator] || x.asInstanceOf[Generator].lhs.toString != m.generator.toString)._1 // trim suffix

      val (xs, ys) = m.parent.qualifiers.span(_ != m.generator)
      m.parent.qualifiers = xs ++ m.child.qualifiers ++ ys.tail

      for (e <- rest; if e.isInstanceOf[ScalaExpr]) substitute(e.asInstanceOf[ScalaExpr], name.toString, term)
    }
  }

  /**
   * Unnests a comprehended head in its parent.
   *
   * Rule Description
   *
   * '''Matching Pattern''':
   * {{{ [[ [[ e | qs' ]] | qs ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e | qs, qs' ]] }}}
   */
  object UnnestHead extends Rule {

    case class RuleMatch(join: MonadJoin, parent: Comprehension, child: Comprehension)

    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
      override def foreach[U](f: (RuleMatch) => U) = {
        for (x <- globalSeq(r.expr)) x match {
          case join@MonadJoin(parent@Comprehension(_, child@Comprehension(_, _, _), _)) =>
            f(RuleMatch(join, parent, child))
          case _ =>
            Unit
        }
      }
    }

    override protected def guard(r: ExpressionRoot, m: RuleMatch) = true //FIXME

    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {
      m.parent.qualifiers = m.parent.qualifiers ++ m.child.qualifiers
      m.parent.head = m.child.head
      m.parent.tpe = m.child.tpe
      r.expr = substitute(r.expr, m.join, m.parent)
    }
  }

  object SimplifyTupleProjection extends Rule {

    val projectionPattern = "_(\\d{1,2})".r

    case class RuleMatch(expr: ScalaExpr)

    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
      override def foreach[U](f: (RuleMatch) => U) = {
        for (x <- globalSeq(r.expr)) x match {
          case expr@ScalaExpr(_, _) =>
            f(RuleMatch(expr))
          case _ =>
            Unit
        }
      }
    }

    override protected def guard(r: ExpressionRoot, m: RuleMatch) = containsPattern(m.expr.tree)

    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {
      m.expr.tree = simplify(m.expr.tree)
    }

    object containsPattern extends Traverser with (Tree => Boolean) {

      var result = false

      override def traverse(tree: Tree): Unit = tree match {
        case Select(Apply(TypeApply(Select(tuple@Select(Ident(TermName("scala")), _), TermName("apply")), types), args), TermName(_)) =>
          val isTupleConstructor = tuple.name.toString.startsWith("Tuple") && tuple.symbol.isModule && tuple.symbol.companion.asClass.baseClasses.contains(symbolOf[Product])
          val isProjectionMethod = tree.symbol.isMethod && projectionPattern.findFirstIn(tree.symbol.name.toString).isDefined
          if (isTupleConstructor && isProjectionMethod) {
            result = true
          } else {
            super.traverse(tree)
          }
        case _ =>
          super.traverse(tree)
      }

      override def apply(tree: Tree): Boolean = {
        containsPattern.result = false
        containsPattern.traverse(tree)
        containsPattern.result
      }
    }

    object simplify extends Transformer with (Tree => Tree) {

      var result = false

      override def transform(tree: Tree): Tree = tree match {
        case Select(Apply(TypeApply(Select(tuple@Select(Ident(TermName("scala")), _), TermName("apply")), types), args), TermName(_)) =>
          val isTupleConstructor = tuple.name.toString.startsWith("Tuple") && tuple.symbol.isModule && tuple.symbol.companion.asClass.baseClasses.contains(symbolOf[Product])
          val projectionMatch = projectionPattern.findFirstMatchIn(tree.symbol.name.toString)
          if (isTupleConstructor && tree.symbol.isMethod && projectionMatch.isDefined) {
            val offset = projectionMatch.get.group(1).toInt
            if (args.size >= offset && types.size >= offset)
              q"${args(offset - 1)}"
            else
              super.transform(tree)
          } else {
            super.transform(tree)
          }
        case _ =>
          super.transform(tree)
      }

      override def apply(tree: Tree): Tree = transform(tree)
    }

  }

  /**
   * Fuses a fold with a child comprehension consisting of a single generator.
   *
   * Rule Description
   *
   * '''Matching Pattern''':
   * {{{ fold( empty, sng, union, [[ e | x ← e' ]] ) }}}
   *
   * '''Rewrite''':
   * {{{ fold( empty, sng[e[x\z]\x], union[e[x\z]\x, e[y\z]\y], e' ]] ) }}}
   */
  object FuseFoldMap extends Rule {

    case class RuleMatch(fold: Fold, map: Comprehension, child: Generator)

    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
      override def foreach[U](f: (RuleMatch) => U) = {
        for (x <- globalSeq(r.expr)) x match {
          case fold@Fold(_, _, _, _, map@Comprehension(_, ScalaExpr(_, _), List(child@Generator(_, _)))) =>
            f(RuleMatch(fold, map, child))
          case _ =>
            Unit
        }
      }
    }

    override protected def guard(r: ExpressionRoot, m: RuleMatch) = true //FIXME

    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {

      val z = m.child.lhs
      val head = m.map.head.asInstanceOf[ScalaExpr]

      val x = substitute(head.tree, z.toString, q"x")
      val y = substitute(head.tree, z.toString, q"y")

      m.fold.sng.tree = substitute(m.fold.sng.tree, "x", x)
      m.fold.union.tree = substitute(m.fold.union.tree, Map("x" -> x, "y" -> y))
      m.fold.in = m.child.rhs
    }
  }

}
