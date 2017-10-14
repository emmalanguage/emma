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

import com.typesafe.config.Config

import scala.reflect.macros.blackbox

class FlinkMacro(val c: blackbox.Context) extends MacroCompiler with FlinkCompiler {

  def onFlinkImpl1[T](e: c.Expr[T]): c.Expr[T] =
    onFlink(loadConfig(configPaths()))(e)

  def onFlinkImpl2[T](config: c.Expr[String])(e: c.Expr[T]): c.Expr[T] =
    onFlink(loadConfig(configPaths(Some(config.tree))))(e)

  def onFlink[T](cfg: Config)(e: c.Expr[T]): c.Expr[T] = {
    // construct the compilation pipeline
    val xfms = transformations(cfg)
    // construct the eval function
    val eval = cfg.getString("emma.compiler.eval") match {
      case "naive" => NaiveEval(pipeline()(xfms: _*)) _
      case "timer" => TimerEval(pipeline()(xfms: _*)) _
    }
    // apply the pipeline to the input tree
    val rslt = eval(e.tree)
    // optionally, print the result
    if (cfg.getBoolean("emma.compiler.print-result")) {
      c.warning(e.tree.pos, api.Tree.show(rslt))
    }
    // wrap the result in an Expr and return it
    c.Expr[T](unTypeCheck(rslt))
  }
}
