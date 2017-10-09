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
package api.emma

import compiler.MacroCompiler

import scala.reflect.macros.blackbox

class QuoteMacro(val c: blackbox.Context) extends MacroCompiler {
  import UniverseImplicits._

  def quoteImpl[T](e: c.Expr[T]): c.Expr[String] = {
    val code = u.showCode(quotePipeline(e))
    // Maximum String literal length in bytes on the JVM.
    if (code.getBytes.length > 0xffff) abort(s"Method too large to quote:\n$code", e.tree.pos)
    c.Expr[String](api.Lit(code))
  }

  lazy val quotePipeline: c.Expr[Any] => u.Tree =
    pipeline()(qualifyThis).compose(_.tree)

  lazy val qualifyThis = TreeTransform("qualifyThis", api.BottomUp.transform {
    case api.This(sym) if sym.isClass && sym.isStatic =>
      api.Ref(sym.asClass.module)
  }._tree)
}
