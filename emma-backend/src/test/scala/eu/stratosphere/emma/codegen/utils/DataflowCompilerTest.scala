package eu.stratosphere.emma.codegen.utils

import org.junit.{Before, Test}

import scala.language.existentials
import scala.reflect.runtime.universe._

class DataflowCompilerTest {

  val mirror = runtimeMirror(getClass.getClassLoader)

  var compiler: DataflowCompiler = _

  @Before def setup() {
    // create a new runtime compiler
    compiler = new DataflowCompiler(mirror)
  }

  @Test def testCompileClassWithParams(): Unit = {

    val tree =
      q"""
       class ClosureTest(val a: Int, val b: Int) {
         def foo(x: (Double, Double)) = (a * x._1, b * x._2, a * b)
       }
       """.asInstanceOf[ImplDef]

    val symbol = compiler.compile(tree).asInstanceOf[ClassSymbol]

    val classMirror = mirror.reflectClass(symbol)
    val classCtorMirror = classMirror.reflectConstructor(symbol.info.member(termNames.CONSTRUCTOR).asMethod)
    val classInstanceMirror = mirror.reflect(classCtorMirror.apply(1, 1))

    val classMethodMirror = classInstanceMirror.reflectMethod(symbol.info.member(TermName("foo")).asMethod)
    val result = classMethodMirror.apply((1.0, 2.0)).asInstanceOf[(Double, Double, Int)]

    assert(result ==(1.0, 2.0, 1))
  }
}
