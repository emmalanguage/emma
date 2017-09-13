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

  override protected val baseConfig = "reference.emma.onFlink.conf" +: super.baseConfig

  def onFlinkImpl1[T](e: c.Expr[T]): c.Expr[T] = {
    val cfg = loadConfig(configPaths())
    val res = pipeline(cfg)(e)
    if (cfg.getBoolean("emma.compiler.print-result")) {
      c.warning(e.tree.pos, api.Tree.show(res))
    }
    c.Expr[T](unTypeCheck(res))
  }

  def onFlinkImpl2[T](config: c.Expr[String])(e: c.Expr[T]): c.Expr[T] = {
    val cfg = loadConfig(configPaths(Some(config.tree)))
    val res = pipeline(cfg)(e)
    if (cfg.getBoolean("emma.compiler.print-result")) {
      c.warning(e.tree.pos, api.Tree.show(res))
    }
    c.Expr[T](unTypeCheck(res))
  }

  private def pipeline(cfg: Config): c.Expr[Any] => u.Tree = {
    val xfms = Seq.newBuilder[u.Tree => u.Tree]
    // standard prefix
    xfms ++= Seq(
      Lib.expand,
      Core.lift
    )
    // optional optimizing rewrites
    if (cfg.getBoolean("emma.compiler.opt.cse")) {
      xfms += Core.cse
    }
    if (cfg.getBoolean("emma.compiler.opt.fold-fusion")) {
      xfms += Optimizations.foldFusion
    }
    if (cfg.getBoolean("emma.compiler.opt.auto-cache")) {
      xfms += Backend.addCacheCalls
    }

    xfms += Comprehension.combine

    xfms += Backend.specialize(FlinkAPI)

    cfg.getString("emma.compiler.lower") match {
      case "trampoline" =>
        xfms += Core.trampoline
      case "dscfInv" =>
        xfms += Core.dscfInv
    }

    xfms += removeShadowedThis
    xfms += prependMemoizeTypeInfoCalls

    // construct the compilation pipeline
    pipeline()(xfms.result(): _*).compose(_.tree)
  }
}
