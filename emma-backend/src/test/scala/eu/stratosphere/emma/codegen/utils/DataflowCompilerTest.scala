package eu.stratosphere.emma.codegen.utils

import org.junit.{Before, Test}

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import scala.language.existentials

class DataflowCompilerTest {

  var compiler: DataflowCompiler = _

  @Before def setup() {
    // create a new runtime compiler
    compiler = new DataflowCompiler()
  }

  @Test def testCompileClassWithParams(): Unit = {

    val tree =
      q"""
       class ClosureTest(val a: Int, val b: Int) {
         def foo(x: (Double, Double)) = (a * x._1, b * x._2, a * b)
       }
       """.asInstanceOf[ImplDef]

    val symbol = compiler.compile(tree).asInstanceOf[ClassSymbol]

    val classMirror = ru.rootMirror.reflectClass(symbol)
    val classCtorMirror = classMirror.reflectConstructor(symbol.info.member(ru.termNames.CONSTRUCTOR).asMethod)
    val classInstanceMirror = ru.rootMirror.reflect(classCtorMirror.apply(1, 1))

    val classMethodMirror = classInstanceMirror.reflectMethod(symbol.info.member(TermName("foo")).asMethod)
    val result = classMethodMirror.apply((1.0, 2.0)).asInstanceOf[(Double, Double, Int)]

    assert(result ==(1.0, 2.0, 1))
  }
}
