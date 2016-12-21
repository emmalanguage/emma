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
package org.emmalanguage
package test

import compiler.MacroCompiler

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
 * Macros needed for the test cases.
 *
 * These are defined here and not in the `test` folder, because they need to be compiled
 * in an earlier compilation run.
 */
class TestMacros(val c: blackbox.Context) extends MacroCompiler {

  val idPipeline: c.Expr[Any] => u.Tree =
    identity().compose(_.tree)

  def removeShadowedThisImpl[T](e: c.Expr[T]): c.Expr[T] = {
    //c.warning(e.tree.pos, "(1) " + c.universe.showCode(e.tree))
    val res = (idPipeline andThen removeShadowedThis andThen reTypeCheck) (e)
    //c.warning(e.tree.pos, "(2) " + c.universe.showCode(res))
    c.Expr(res)
  }

  lazy val reTypeCheck: u.Tree => u.Tree = tree => typeCheck(unTypeCheck(tree))
}

object TestMacros {

  final def removeShadowedThis[T](e: T): T = macro TestMacros.removeShadowedThisImpl[T]

}
