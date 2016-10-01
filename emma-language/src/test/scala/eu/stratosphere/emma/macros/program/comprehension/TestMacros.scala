/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.controlflow.ControlFlow
import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class TestMacros(val c: blackbox.Context) extends ControlFlow with Comprehension {
  import universe._
  import syntax._

  /** Translates an Emma expression to an Algorithm. */
  def parallelize[T: c.WeakTypeTag](expr: Expr[T]) = {

    // Create a normalized version of the original tree
    val normalized = normalize(expr.tree)

    // ----------------------------------------------------------------------
    // Code analysis
    // ----------------------------------------------------------------------

    // 1. Create control flow graph
    implicit var cfGraph = createControlFlowGraph(normalized)

    // 2. Identify and isolate maximal comprehensions
    implicit var comprehensionView = createComprehensionView(cfGraph)

    // ----------------------------------------------------------------------
    // Code optimizations
    // ----------------------------------------------------------------------

    // 1. Inline comprehensions
    val optimized = inlineComprehensions(normalized)
    cfGraph = createControlFlowGraph(optimized)
    comprehensionView = createComprehensionView(cfGraph)

    // normalize filter predicates to CNF
    normalizePredicates(optimized)

    // 2. Apply Fold-Group-Fusion where possible
    foldGroupFusion(optimized)

    // ----------------------------------------------------------------------
    // Final object assembly
    // ----------------------------------------------------------------------

    // construct algorithm object
    q"""new _root_.eu.stratosphere.emma.api.Algorithm[${c.weakTypeOf[T]}] {
      import _root_.scala.reflect._

      def run(engine: _root_.eu.stratosphere.emma.runtime.Engine): ${c.weakTypeOf[T]} =
        engine match {
          case _: _root_.eu.stratosphere.emma.runtime.Native => runNative()
          case _ => runParallel(engine)
        }

        private def runNative(): ${c.weakTypeOf[T]} =
          { ${expr.tree.unTypeChecked} }

        private def runParallel(engine: _root_.eu.stratosphere.emma.runtime.Engine):
          ${c.weakTypeOf[T]} = { ${compile(optimized, cfGraph, comprehensionView)} }
    }""".typeChecked
  }

  /** Translates an Emma expression to an Algorithm. */
  def comprehend[T: c.WeakTypeTag](expr: Expr[T]) = {

    // Create a normalized version of the original tree
    val normalized = normalize(expr.tree)

    // ----------------------------------------------------------------------
    // Code analysis
    // ----------------------------------------------------------------------

    // 1. Create control flow graph
    implicit var cfGraph = createControlFlowGraph(normalized)

    // 2. Identify and isolate maximal comprehensions
    implicit var comprehensionView = createComprehensionView(cfGraph)

    // ----------------------------------------------------------------------
    // Code optimizations
    // ----------------------------------------------------------------------

    // 1. Inline comprehensions
    val optimized = inlineComprehensions(normalized)
    cfGraph = createControlFlowGraph(optimized)
    comprehensionView = createComprehensionView(cfGraph)

    // 2. Apply Fold-Group-Fusion where possible
    foldGroupFusion(optimized)

    // ----------------------------------------------------------------------
    // Final result assembly
    // ----------------------------------------------------------------------

    q"""{ ..${
      for (term <- comprehensionView.terms)
        yield q"""
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
          println(${showCode(compile(optimized, cfGraph, comprehensionView))})
          println("~" * 80)
          println("")"""
    }}""".typeChecked
  }

  /** Comprehend and unComprehend the algorithm to test the IR. */
  def reComprehend[T: c.WeakTypeTag](expr: Expr[T]) = {

    // Create a normalized version of the original tree
    val normalized = normalize(expr.tree)

    // ----------------------------------------------------------------------
    // Code analysis
    // ----------------------------------------------------------------------

    // 1. Create control flow graph
    implicit var cfGraph = createControlFlowGraph(normalized)

    // 2. Identify and isolate maximal comprehensions
    implicit var comprehensionView = createComprehensionView(cfGraph)

    // ----------------------------------------------------------------------
    // Code optimizations
    // ----------------------------------------------------------------------

    // 1. Inline comprehensions
    val optimized = inlineComprehensions(normalized)
    cfGraph = createControlFlowGraph(optimized)
    comprehensionView = createComprehensionView(cfGraph)

    // 2. Apply Fold-Group-Fusion where possible
    foldGroupFusion(optimized)

    // ----------------------------------------------------------------------
    // Final object assembly
    // ----------------------------------------------------------------------

    val reComprehender = new ReComprehender(comprehensionView)

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
        ${c.weakTypeOf[T]} = { ${compile(optimized, cfGraph, comprehensionView)} }
    }""".typeChecked
  }

  class ReComprehender(comprehensionView: ComprehensionView) extends Transformer {
    override def transform(tree: Tree) = comprehensionView.getByTerm(tree) match {
      case Some(term) => unComprehend(term.comprehension.expr)
      case None => super.transform(tree)
    }
  }
}
