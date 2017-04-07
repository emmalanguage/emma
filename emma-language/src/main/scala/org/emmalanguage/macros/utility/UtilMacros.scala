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
package macros.utility

import compiler.MacroCompiler

import scala.reflect.macros.blackbox

class UtilMacros(val c: blackbox.Context) extends MacroCompiler {

  val idPipeline: c.Expr[Any] => u.Tree =
    identity(typeCheck = false).compose(_.tree)

  import Core.{Lang => core}

  def visualize[T](e: c.Expr[T]) = {
    browse(e.tree)
    e
  }

  def prettyPrint[T](e: c.Expr[T]): c.Expr[String] = c.Expr(
    core.Lit(
      Core.prettyPrint(idPipeline(e))
    )
  )
}
