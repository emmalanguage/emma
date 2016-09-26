package eu.stratosphere.emma
package compiler

import api.DataBag
import lang.TreeMatchers

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FreeSpec, Matchers}
import java.util.{Properties, UUID}

/**
 * Common methods and mixins for all compier specs
 */
trait BaseCompilerSpec extends FreeSpec with Matchers with PropertyChecks with TreeMatchers {

  val compiler = new RuntimeCompiler()

  import compiler._

  // ---------------------------------------------------------------------------
  // Common transformation pipelines
  // ---------------------------------------------------------------------------

  /** Checks if the given tree compiles, and returns the given tree. */
  val checkCompile: u.Tree => u.Tree = (tree: u.Tree) => {
    if (BaseCompilerSpec.compileSpecPipelines) {
      val wrapped = wrapInClass(tree)
      val showed = u.showCode(wrapped)
      compiler.compile(parse(showed))
      compiler.typeCheck(u.reify {}.tree) // This is a workaround for https://issues.scala-lang.org/browse/SI-9932
    }
    tree
  }

  val idPipeline: u.Expr[Any] => u.Tree = {
      (_: u.Expr[Any]).tree
    } andThen {
      compiler.identity(typeCheck = true)
    } andThen {
      checkCompile
    }

  /**
   * Combines a sequence of `transformations` into a pipeline with pre- and post-processing.
   * If the IT maven profile is set, then it also checks if the resulting code is valid by compiling it.
   */
  def pipeline(typeCheck: Boolean = false, withPre: Boolean = true, withPost: Boolean = true)
              (transformations: (u.Tree => u.Tree)*): u.Tree => u.Tree = {
    compiler.pipeline(typeCheck, withPre, withPost)(transformations: _*) andThen checkCompile
  }

  // ---------------------------------------------------------------------------
  // Common value definitions used in compiler tests
  // ---------------------------------------------------------------------------

  val x = 42
  val y = "The answer to life, the universe and everything"
  val t = (x, y)
  val xs = DataBag(Seq(1, 2, 3))
  val ys = DataBag(Seq(1, 2, 3))

  // ---------------------------------------------------------------------------
  // Utility functions
  // ---------------------------------------------------------------------------

  def time[A](f: => A, name: String = "") = {
    val s = System.nanoTime
    val ret = f
    println(s"$name time: ${(System.nanoTime - s) / 1e6}ms".trim)
    ret
  }

  private def wrapInClass(tree: u.Tree): u.Tree = {
    import universe._
    val params = api.Tree.closure(tree).map{ sym =>
      val name = sym.name
      val tpt = sym.typeSignature
      q"val $name: $tpt"
    }
    q"""
      class ${api.TypeName(UUID.randomUUID().toString)} {
        def run(..$params) = {
          $tree
        }
      }
     """
  }
}

object BaseCompilerSpec {

  import resource._

  val configLocation = "/test_config.properties"

  /**
   * Whether spec pipelines should try a toolbox compilation at the end, to potentially catch more bugs.
   * (Set by the IT maven profile; makes specs run about 5 times slower.)
   */
  lazy val compileSpecPipelines = props
    .getProperty("compile-spec-pipelines")
    .toBoolean

  lazy val props = {
    val p = new Properties()
    for {
      r <- managed(classOf[BaseCompilerSpec].getResourceAsStream(configLocation))
    } p.load(r)
    p
  }
}