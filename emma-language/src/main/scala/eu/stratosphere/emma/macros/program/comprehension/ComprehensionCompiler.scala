package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.controlflow.ControlFlowModel
import eu.stratosphere.emma.macros.program.util.ProgramUtils

import scala.reflect.macros._

private[emma] trait ComprehensionCompiler[C <: blackbox.Context]
  extends ContextHolder[C]
  with ControlFlowModel[C]
  with ComprehensionModel[C]
  with ProgramUtils[C] {

  import c.universe._

  /**
   * Expands a comprehended term in the compiled driver.
   *
   * @param tree The original program tree.
   * @param cfGraph The control flow graph representation of the tree.
   * @param comprehensionStore A store containing the comprehended terms in the tree.
   * @param t The comprehended term to be expanded.
   * @return A tree representing the expanded comprehension.
   */
  def expand(tree: Tree, cfGraph: CFGraph, comprehensionStore: ComprehensionStore)(t: ComprehendedTerm) = {
    // FIXME: avoid name collisions by explicitly prefixing ir types with ir.*
    q"""{
      val comprehension = {

        // required imports
        import scala.reflect.runtime.universe._
        import eu.stratosphere.emma.ir._
        import eu.stratosphere.emma.optimizer._

        val engine = new rewrite.OptimizerRewriteEngine()
        val root = engine.rewrite(ExpressionRoot(${serialize(t.comprehension.expr)}))

        println(prettyprint(root.expr))
      }

      0
    }"""
  }

  /**
   * Serializes a macro-level IR tree as code constructing an equivalent runtime-level IR tree.
   *
   * @param e The expression in IR to be serialized.
   * @return
   */
  private def serialize(e: Expression): Tree = {
    e match {
      // Monads
      case MonadUnit(expr) =>
        q"MonadUnit(${serialize(expr)})(scala.reflect.runtime.universe.typeTag[${e.tpe}])"

      case MonadJoin(expr) =>
        q"MonadJoin(${serialize(expr)})(scala.reflect.runtime.universe.typeTag[${e.tpe}])"

      case Comprehension(_, head, qualifiers) =>
        q"""
      {
        // MC qualifiers
        val qualifiers = scala.collection.mutable.ListBuffer[Qualifier]()
        ..${for (q <- qualifiers) yield q"qualifiers += ${serialize(q)}"}

        // MC head
        val head = ${serialize(head)}

        // MC constructor
        Comprehension(head, qualifiers.toList)(scala.reflect.runtime.universe.typeTag[${e.tpe}])
      }
      """

      // Qualifiers
      case Filter(expr) =>
        q"Filter(${serialize(expr)})"

      case Generator(lhs, rhs) =>
        q"Generator(${lhs.toString}, ${serialize(rhs)})(scala.reflect.runtime.universe.typeTag[${e.tpe}])"

      // Environment & Host Language Connectors
      case ScalaExpr(env, t) =>
        val tps = (for (e <- referencedEnv(t, env)) yield e match {
          case v@ValDef(m, name, t, rhs) => name.toString -> q"reify( {$v; $name} )"
          case _ => throw new IllegalStateException()
        }).toMap
        q"ScalaExpr($tps, { ..${referencedEnv(t, env)}; reify { ${freeEnv(t, env)} } })(scala.reflect.runtime.universe.typeTag[${e.tpe}])"

      case Read(_, location, format) =>
        q"Read[${e.tpe}]($location, $format)(scala.reflect.runtime.universe.typeTag[${e.tpe}])"

      case Write(location, format, in) =>
        q"Write[${e.tpe}]($location, $format, ${serialize(in)})(scala.reflect.runtime.universe.typeTag[${e.tpe}])"
    }
  }
}
