package eu.stratosphere.emma.compiler

import eu.stratosphere.emma.api.DataBag
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FreeSpec, Matchers}

/**
 * Common methods and mixins for all compier specs
 */
trait BaseCompilerSpec extends FreeSpec with Matchers with PropertyChecks {

  val compiler = new RuntimeCompiler()

  import compiler._
  import universe._

  // ---------------------------------------------------------------------------
  // Common transformation pipelines
  // ---------------------------------------------------------------------------

  def typeCheck[T]: Expr[T] => Tree = {
    (_: Expr[T]).tree
  } andThen {
    Type.check(_: Tree)
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
}
