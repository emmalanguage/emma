package eu.stratosphere.emma.compiler

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FreeSpec, Matchers}

/**
 * Common methods and mixins for all compier specs
 */
trait BaseCompilerSpec extends FreeSpec with Matchers with PropertyChecks {

  import scala.reflect.runtime.universe._

  val compiler = new RuntimeCompiler()

  implicit object TreeEquality extends org.scalactic.Equality[Tree] {
    override def areEqual(a: Tree, b: Any): Boolean = b match {
      case b: Tree if a equalsStructure b => true
      case _ => false
    }
  }

  def time[A](f: => A) = {
    val s = System.nanoTime
    val ret = f
    println(s"time: ${(System.nanoTime - s) / 1e6}ms")
    ret
  }
}
