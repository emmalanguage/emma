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
package compiler

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class SparkMacro(val c: blackbox.Context) extends MacroCompiler with SparkCompiler {

  def parallelizeImpl[T](e: c.Expr[T]): c.Expr[T] = {
    val res = parallelizePipeline(e)
    //c.warning(e.tree.pos, Core.prettyPrint(res))
    c.Expr[T]((removeShadowedThis andThen unTypeCheck) (res))
  }

  private lazy val parallelizePipeline: c.Expr[Any] => u.Tree =
    pipeline()(
      LibSupport.expand,
      Core.lift,
      Backend.addCacheCalls,
      Comprehension.combine,
      Backend.specialize(SparkAPI),
      Core.inlineLetExprs,
      Core.trampoline
    ).compose(_.tree)
}
