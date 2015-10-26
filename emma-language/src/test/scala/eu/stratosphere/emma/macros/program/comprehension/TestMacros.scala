package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.api.Algorithm
import eu.stratosphere.emma.macros.program.controlflow.ControlFlow
import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class TestMacros(val c: blackbox.Context) extends ControlFlow with Comprehension {
  import universe._
  import syntax._

  /**
   * Translates an Emma expression to an Algorithm.
   *
   * @return
   */
  def parallelize[T: c.WeakTypeTag](e: Expr[T]): Expr[Algorithm[T]] = {

    // Create a normalized version of the original tree
    val normalizedTree = normalize(e.tree)

    // ----------------------------------------------------------------------
    // Code analysis
    // ----------------------------------------------------------------------

    // 1. Create control flow graph
    implicit var cfGraph = createControlFlowGraph(normalizedTree)

    // 2. Identify and isolate maximal comprehensions
    implicit var comprehensionView = createComprehensionView(cfGraph)

    // ----------------------------------------------------------------------
    // Code optimizations
    // ----------------------------------------------------------------------

    // 1. Inline comprehensions
    val optimizedTree = inlineComprehensions(normalizedTree)
    cfGraph = createControlFlowGraph(optimizedTree)
    comprehensionView = createComprehensionView(cfGraph)

    // normalize filter predicates to CNF
    normalizePredicates(optimizedTree)

    // 2. Apply Fold-Group-Fusion where possible
    foldGroupFusion(optimizedTree)

    // ----------------------------------------------------------------------
    // Final object assembly
    // ----------------------------------------------------------------------

    // construct algorithm object
    val algorithmCode =
      q"""
      new eu.stratosphere.emma.api.Algorithm[${c.weakTypeOf[T]}] {
         import _root_.scala.reflect._

         def run(engine: eu.stratosphere.emma.runtime.Engine): ${c.weakTypeOf[T]} = engine match {
           case _: eu.stratosphere.emma.runtime.Native => runNative()
           case _ => runParallel(engine)
         }

         private def runNative(): ${c.weakTypeOf[T]} = {
           ${e.tree.unTypeChecked}
         }

         private def runParallel(engine: eu.stratosphere.emma.runtime.Engine): ${c.weakTypeOf[T]} = {
           ${compile(optimizedTree, cfGraph, comprehensionView)}
         }
      }
      """

    val ac = c.Expr[Algorithm[T]](c.typecheck(algorithmCode))
    ac
  }

  /**
   * Translates an Emma expression to an Algorithm.
   *
   * @return
   */
  def comprehend[T: c.WeakTypeTag](e: Expr[T]): Expr[Unit] = {

    // Create a normalized version of the original tree
    val normalizedTree = normalize(e.tree)

    // ----------------------------------------------------------------------
    // Code analysis
    // ----------------------------------------------------------------------

    // 1. Create control flow graph
    implicit var cfGraph = createControlFlowGraph(normalizedTree)

    // 2. Identify and isolate maximal comprehensions
    implicit var comprehensionView = createComprehensionView(cfGraph)

    // ----------------------------------------------------------------------
    // Code optimizations
    // ----------------------------------------------------------------------

    // 1. Inline comprehensions
    val optimizedTree = inlineComprehensions(normalizedTree)
    cfGraph = createControlFlowGraph(optimizedTree)
    comprehensionView = createComprehensionView(cfGraph)

    // 2. Apply Fold-Group-Fusion where possible
    foldGroupFusion(optimizedTree)

    // ----------------------------------------------------------------------
    // Final result assembly
    // ----------------------------------------------------------------------

    val code =
      q"""
      { ..${
        for (t <- comprehensionView.terms) yield
          q"""
          println("~" * 80)
          println("~ Comprehension `" + ${t.id.toString} + "` (original):")
          println("~" * 80)
          println(${t.comprehension.toString})
          println("~" * 80)
          println("~ Comprehension `" + ${t.id.toString} + "` (combinatros):")
          println("~" * 80)
          println(${combine(t.comprehension).toString})
          println("~" * 80)
          println("")
          println("~ Comprehension `" + ${t.id.toString} + "` (IR):")
          println("~" * 80)
          println(${showCode(compile(optimizedTree, cfGraph, comprehensionView))})
          println("~" * 80)
          println("")
          """
      }}"""


    c.Expr[Unit](c.typecheck(code))
  }

  /** Comprehend and unComprehend the algorithm to test the IR. */
  def reComprehend[T: c.WeakTypeTag](e: Expr[T]) = {
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
    cfGraph = createControlFlowGraph(optimized)
    compView = createComprehensionView(cfGraph)
    // normalize filter predicates to CNF
    // normalizePredicates(optimized)
    // 2. Apply Fold-Group-Fusion where possible
    foldGroupFusion(optimized)

    // ----------------------------------------------------------------------
    // Final object assembly
    // ----------------------------------------------------------------------

    val reComprehender = new ReComprehender(compView)

    // construct algorithm object
    q"""new _root_.eu.stratosphere.emma.api.Algorithm[${c.weakTypeOf[T]}] {
        import _root_.scala.reflect._

      def run(engine: _root_.eu.stratosphere.emma.runtime.Engine): ${c.weakTypeOf[T]} =
        engine match {
          case _: _root_.eu.stratosphere.emma.runtime.Native => runNative()
          case _ => runParallel(engine)
        }

      private def runNative(): ${c.weakTypeOf[T]} =
        { ${reComprehender.transform(optimized).unTypeChecked} }

      private def runParallel(engine: _root_.eu.stratosphere.emma.runtime.Engine):
        ${c.weakTypeOf[T]} = { ${compile(optimized, cfGraph, compView)} }
    }""".typeChecked
  }

  class ReComprehender(compView: ComprehensionView) extends Transformer {
    override def transform(tree: Tree): Tree = compView getByTerm tree match {
      case Some(term) => unComprehend(term.comprehension.expr)
      case None => super.transform(tree)
    }
  }
}
