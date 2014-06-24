package eu.stratosphere.emma.macros.program.rewrite

import _root_.eu.stratosphere.emma.macros.program.ContextHolder
import _root_.eu.stratosphere.emma.macros.program.ir.IntermediateRepresentation
import _root_.eu.stratosphere.emma.macros.program.util.ProgramUtils
import _root_.eu.stratosphere.emma.rewrite.RewriteEngine

import _root_.scala.reflect.macros.blackbox.Context

trait MacroRewriteEngine[C <: Context]
  extends ContextHolder[C]
  with IntermediateRepresentation[C]
  with ProgramUtils[C]
  with RewriteEngine {

  import c.universe._

  val rules: List[Rule] = List(UnnestHead, UnnestGenerator)

  object UnnestGenerator extends Rule {

    case class RuleMatch(parent: Comprehension, generator: ComprehensionGenerator, child: Comprehension)

    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
      override def foreach[U](f: (RuleMatch) => U) = {
        for (x <- sequence(r.expr)) x match {
          case parent@Comprehension(_, _, qualifiers) =>
            for (y <- qualifiers) y match {
              case generator@ComprehensionGenerator(_, child@Comprehension(_, ScalaExpr(_, _), _)) =>
                f(RuleMatch(parent, generator, child))
              case _ => Unit
            }
          case _ => Unit
        }
      }
    }

    override protected def guard(r: ExpressionRoot, m: RuleMatch) = {
      val parentIsSink = m.parent.qualifiers.size == 1 && m.parent.head.isInstanceOf[ScalaExpr] && (m.parent.head.asInstanceOf[ScalaExpr].tree match {
        case Apply(Select(Ident(TermName("ofmt")), TermName("write")), Ident(TermName(name)) :: Nil) => name.startsWith("snk$record$")
        case _ => false
      })

      val childIsSource = m.child.qualifiers.size == 2 && m.child.head.isInstanceOf[ScalaExpr] && (m.child.head.asInstanceOf[ScalaExpr].tree match {
        case Ident(TermName(name)) => name.startsWith("src$record$")
        case _ => false
      })

      !(childIsSource || parentIsSink)
    }

    override protected def fire(r: ExpressionRoot, m: RuleMatch) = {
      val name = m.generator.lhs
      val term = m.child.head.asInstanceOf[ScalaExpr]
      val rest = sequence(m.parent)
        .span(_ != m.generator)._2.tail // trim prefix
        .span(x => !x.isInstanceOf[ComprehensionGenerator] || x.asInstanceOf[ComprehensionGenerator].lhs.toString != m.generator.toString)._1 // trim suffix

      val (xs, ys) = m.parent.qualifiers.span(_ != m.generator)
      m.parent.qualifiers = xs ++ m.child.qualifiers ++ ys.tail

      for (e <- rest; if e.isInstanceOf[ScalaExpr]) substitute(e.asInstanceOf[ScalaExpr], name.toString, term)
    }
  }

  object UnnestHead extends Rule {

    case class RuleMatch(join: MonadJoin, parent: Comprehension, child: Comprehension)

    override protected def bind(r: ExpressionRoot) = new Traversable[RuleMatch] {
      override def foreach[U](f: (RuleMatch) => U) = {
        for (x <- sequence(r.expr)) x match {
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
      m.parent.monad = m.child.monad
      r.expr = substitute(r.expr, m.join, m.parent)
    }
  }

}
