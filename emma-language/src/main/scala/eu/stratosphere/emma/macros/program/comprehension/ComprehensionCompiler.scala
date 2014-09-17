package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.util.ProgramUtils

import scala.collection.mutable
import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.controlflow.ControlFlowModel

import scala.reflect.macros._

private[emma] trait ComprehensionCompiler[C <: blackbox.Context]
  extends ContextHolder[C]
  with ComprehensionModel[C]
  with ProgramUtils[C] {

  import c.universe._

  /**
   * Serializes a macro-level IR tree as code constructing an equivalent runtime-level IR tree.
   *
   * @param e The expression in IR to be serialized.
   * @return
   */
  private def serialize(e: Expression): Tree = e match {
    case MonadUnit(expr) =>
      q"MonadUnit(${serialize(expr)})"
    case MonadJoin(expr) =>
      q"MonadJoin(${serialize(expr)})"
    case Filter(expr) =>
      q"Filter(${serialize(expr)})"
    case Generator(lhs, rhs) =>
      q"{ val rhs = ${serialize(rhs)}; ComprehensionGenerator(${lhs.toString}, rhs) }"
    case ScalaExpr(env, t) =>
      q"ScalaExpr(${for (v <- referencedEnv(t, env)) yield v.name.toString}, { ..${referencedEnv(t, env)}; reify { ${freeEnv(t, env)} } })"
    case Comprehension(tpe, head, qualifiers) =>
      q"""
        {
          // MC qualifiers
          val qualifiers = ListBuffer[Qualifier]()
          ..${for (q <- qualifiers) yield q"qualifiers += ${serialize(q)}"}

          // MC head
          val head = ${serialize(head)}

          // MC constructor
          Comprehension($tpe, head, qualifiers.toList)
        }
         """
  }
}
