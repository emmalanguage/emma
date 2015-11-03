package eu.stratosphere.emma.codegen.utils

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.language.existentials
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._

@RunWith(classOf[JUnitRunner])
class DataflowCompilerTest extends FlatSpec with Matchers {
  lazy val compiler = new DataflowCompiler(currentMirror)

  "The dataflow compiler" should "compile a class with parameters" in {
    val tree = q"""class ClosureTest(val a: Int, val b: Int) {
      def foo(x: (Double, Double)) = (a * x._1, b * x._2, a * b)
    }""".asInstanceOf[ImplDef]

    val symbol = compiler.compile(tree).asInstanceOf[ClassSymbol]
    val clazz = currentMirror.reflectClass(symbol)
    val constructor = clazz.reflectConstructor(symbol.info.member(termNames.CONSTRUCTOR).asMethod)
    val instance = currentMirror.reflect(constructor(1, 1))
    val foo = instance.reflectMethod(symbol.info.member(TermName("foo")).asMethod)
    foo((1.0, 2.0)) should be (1.0, 2.0, 1)
  }
}
