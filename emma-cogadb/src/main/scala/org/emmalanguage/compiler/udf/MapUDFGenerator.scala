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
import org.emmalanguage.compiler.udf.common._

import scala.collection.mutable
import scala.reflect.runtime.universe._
import internal.reificationSupport._

class MapUDFGenerator(ast: Tree, symbolTable: Map[String, String], child: Op) extends JsonIRGenerator[MapUdf]
  with AnnotatedCCodeGenerator with TypeHelper {

  private val outputs = mutable.ListBuffer.empty[MapUdfOutAttr]

  private val localVars = mutable.Map.empty[TermName, (Type, TermName)]

  def generate: MapUdf = {
    val mapUdfCode = generateAnnotatedCCode(symbolTable, ast, true)
    MapUdf(outputs, mapUdfCode.map(MapUdfCode(_)), child)
  }

  override def generateOutputExpr(col: TypeName): String = s"#<OUT>.$col#"

  override protected def freshVarName = freshTermName("map_udf_local_var_")

  override protected def newUDFOutput(tpe: Type, infix: String = ""): TypeName = {
    val freshOutputName = freshTypeName(s"MAP_UDF_RES_$infix".toUpperCase)
    outputs += MapUdfOutAttr(tpe.toJsonAttributeType, s"$freshOutputName", s"$freshOutputName")
    freshOutputName
  }

  override protected def basicTypeColumnIdentifier: String = "VALUE"
}
