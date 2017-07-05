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
package compiler.udf

import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.UDFCompiler.UDFType.UDFType
import org.emmalanguage.compiler.udf.common._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

object UDFCompiler {

  object UDFType extends Enumeration {
    type UDFType = Value
    val Map, Filter, Fold = Value
  }

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  def compile(child: Op, udfType: UDFType, symbolTable: Map[String, String], udfs: String*): Node = udfType match {
    case UDFType.Map => {
      val ast = tb.typecheck(q"${udfs.head}")
      val udfTransformer = new UDFTransformer(MapUDFClosure(ast, symbolTable, child))
      udfTransformer.transform
    }
    case UDFType.Filter => {
      val ast = tb.typecheck(q"${udfs.head}")
      val udfTransformer = new UDFTransformer(FilterUDFClosure(ast, symbolTable, child))
      udfTransformer.transform
    }
    case UDFType.Fold => {
      val zElem = tb.typecheck(q"${udfs.head}")
      val sngAst = tb.typecheck(q"${udfs(1)}")
      val uniAst = tb.typecheck(q"${udfs(2)}")
      val udfTransformer = new UDFTransformer(FoldUDFClosure(zElem, sngAst, uniAst, symbolTable, child))
      udfTransformer.transform
    }
  }
}
