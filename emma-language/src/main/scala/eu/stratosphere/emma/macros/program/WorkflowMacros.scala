package eu.stratosphere.emma.macros.program

import eu.stratosphere.emma.api.Algorithm
import eu.stratosphere.emma.macros.program.comprehension.Comprehension
import eu.stratosphere.emma.macros.program.controlflow.ControlFlow
import eu.stratosphere.emma.runtime.{Engine, Native}
import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class WorkflowMacros(val c: blackbox.Context) extends ControlFlow with Comprehension {
  import universe._

  val ENGINE = typeOf[Engine]
  val NATIVE = typeOf[Native]

  /** Translate an Emma expression to an [[Algorithm]]. */
  // TODO: Add more comprehensive ScalaDoc
  def parallelize[T: c.WeakTypeTag](e: Expr[T]) = {

    // Create a normalized version of the original tree
    val normalized = normalize(e.tree)

    // ----------------------------------------------------------------------
    // Code analysis
    // ----------------------------------------------------------------------

    // 1. Create control flow graph
    implicit var cfGraph = createControlFlowGraph(normalized)

    // 2. Identify and isolate maximal comprehensions
    implicit var compView = createComprehensionView(cfGraph)

    // ----------------------------------------------------------------------
    // Code optimizations
    // ----------------------------------------------------------------------

    // 1. Inline comprehensions
    val optimized = inlineComprehensions(normalized)
    cfGraph       = createControlFlowGraph(optimized)
    compView      = createComprehensionView(cfGraph)

    // 2. Normalize filter predicates to CNF
    // normalizePredicates(optimized)

    // 3. Apply Fold-Group-Fusion where possible
    foldGroupFusion(optimized)

    // ----------------------------------------------------------------------
    // Final object assembly
    // ----------------------------------------------------------------------

    // Construct algorithm object
    q"""new _root_.eu.stratosphere.emma.api.Algorithm[${weakTypeOf[T]}] {
      import _root_.scala.reflect._

      def run(engine: $ENGINE): ${weakTypeOf[T]} = engine match {
        case _: $NATIVE => runNative()
        case _ => runParallel(engine)
      }

      private def runNative(): ${weakTypeOf[T]} = {
        ${e.tree.unTypeChecked}
      }

      private def runParallel(engine: $ENGINE): ${weakTypeOf[T]} = {
        ${compile(optimized, cfGraph, compView)}
      }
    }""".typeChecked
  }

  /** Translate an Emma expression to an [[Algorithm]]. */
  // TODO: Add more comprehensive ScalaDoc
  def comprehend[T: c.WeakTypeTag](e: Expr[T]) = {

    // Create a normalized version of the original tree
    val normalized = normalize(e.tree)

    // ----------------------------------------------------------------------
    // Code analysis
    // ----------------------------------------------------------------------

    // 1. Create control flow graph
    implicit var cfGraph = createControlFlowGraph(normalized)

    // 2. Identify and isolate maximal comprehensions
    implicit var compView = createComprehensionView(cfGraph)

    // ----------------------------------------------------------------------
    // Code optimizations
    // ----------------------------------------------------------------------

    // 1. Inline comprehensions
    val optimized = inlineComprehensions(normalized)
    cfGraph       = createControlFlowGraph(optimized)
    compView      = createComprehensionView(cfGraph)

    // 2. Normalize filter predicates to CNF
    // normalizePredicates(optimized)

    // 3. Apply Fold-Group-Fusion where possible
    foldGroupFusion(optimized)

    // ----------------------------------------------------------------------
    // Final result assembly
    // ----------------------------------------------------------------------

    q"""{ ..${for (term <- compView.terms) yield q"""
      println("~" * 80)
      println("~ Comprehension `" + ${term.id.toString} + "` (original):")
      println("~" * 80)
      println(${term.comprehension.toString})
      println("~" * 80)
      println("~ Comprehension `" + ${term.id.toString} + "` (combinatros):")
      println("~" * 80)
      println(${combine(term.comprehension).toString})
      println("~" * 80)
      println("")
      println("~ Comprehension `" + ${term.id.toString} + "` (IR):")
      println("~" * 80)
      println(${showCode(compile(optimized, cfGraph, compView))})
      println("~" * 80)
      println("")"""}
    }""".typeChecked
  }
}
