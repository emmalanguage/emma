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
