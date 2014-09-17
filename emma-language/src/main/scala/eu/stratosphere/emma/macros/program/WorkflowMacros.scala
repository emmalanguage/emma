package eu.stratosphere.emma.macros.program


import eu.stratosphere.emma.api.Algorithm
import eu.stratosphere.emma.macros.program.controlflow.ControlFlow
import eu.stratosphere.emma.macros.program.comprehension.Comprehension
import eu.stratosphere.emma.macros.program.comprehension.rewrite.ComprehensionNormalization

import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class WorkflowMacros(val c: blackbox.Context) {

  /**
   * Entry macro for emma algorithms.
   */
  def parallelize[T: c.WeakTypeTag](e: c.Expr[T]): c.Expr[Algorithm[T]] = {
    new LiftHelper[c.type](c).parallelize[T](e)
  }

  private class LiftHelper[C <: blackbox.Context](val c: C)
    extends ContextHolder[c.type]
    with ControlFlow[c.type]
    with Comprehension[c.type]
    with ComprehensionNormalization[c.type] {

    import c.universe._

    /**
     * Translates an Emma expression to an Algorithm.
     *
     * @return
     */
    def parallelize[T: c.WeakTypeTag](root: Expr[T]): Expr[Algorithm[T]] = {

      val rootTree = c.untypecheck(root.tree)

      // ----------------------------------------------------------------------
      // Code analysis
      // ----------------------------------------------------------------------

      // 1. Create control flow graph
      val cfGraph = createCFG(rootTree)

      // 2. Identify and isolate maximal comprehensions
      val comprehensionStore = createComprehensionStore(cfGraph)

      // 3. Analyze variable usage

      // ----------------------------------------------------------------------
      // Code optimizations
      // ----------------------------------------------------------------------

      // 1. Comprehension rewrite (TODO)

      // 2. Derive logical plans (TODO)

      // ----------------------------------------------------------------------
      // Final object assembly
      // ----------------------------------------------------------------------

      // construct algorithm object
      val algorithmCode =
        q"""
        object __emmaAlgorithm extends eu.stratosphere.emma.api.Algorithm[${c.weakTypeOf[T]}] {

           // required imports
           import eu.stratosphere.emma.api._
           import eu.stratosphere.emma.ir

           def run(engine: runtime.Engine): ${c.weakTypeOf[T]} = engine match {
             case runtime.Native => runNative()
             case _ => runParallel(engine)
           }

           private def runNative(): ${c.weakTypeOf[T]} = $rootTree

           private def runParallel(engine: runtime.Engine): ${c.weakTypeOf[T]} = { ..${compileDriver[T](cfGraph)} }
        }
        """

      // construct and return a block that returns a Workflow using the above list of sinks
      val block = Block(List(algorithmCode), c.parse("__emmaAlgorithm"))
      c.Expr[Algorithm[T]](c.typecheck(block))
    }

//    /**
//     * Lifts the root block of an Emma program.
//     *
//     * @param e The root block AST to be lifted.
//     * @return
//     */
//    private def liftRootBlock(e: Block): Block = {
//
//      // recursively lift to MC syntax starting from the sinks
//      val sinks = (for (s <- extractSinkExprs(e.expr)) yield ExpressionRoot(lift(e, Nil)(resolve(e)(s)))).toList
//
//      // a list of statements for the root block of the translated MC expression
//      val stats = ListBuffer[Tree]()
//      // 1) add required imports
//      stats += c.parse("import eu.stratosphere.emma.ir._")
//      stats += c.parse("import scala.collection.mutable.ListBuffer")
//      stats += c.parse("import scala.reflect.runtime.universe._")
//      // 2) initialize translated sinks list
//      stats += c.parse("val sinks = ListBuffer[Comprehension]()")
//      // 3) add the translated MC expressions for all sinks
//      for (s <- sinks) {
//        stats += q"sinks += ${serialize(rewrite(s).expr)}"
//      }
//
//      // construct and return a block that returns a Workflow using the above list of sinks
//      Block(stats.toList, c.parse( """Workflow("Emma Workflow", sinks.toList)"""))
//    }

  }

}