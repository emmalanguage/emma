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

import compiler.lang.cogadb.ast._

import scala.reflect.runtime.universe._

object common {

  sealed abstract class UDFClosure(symTbl: Map[String, String])
  
  case class MapUDFClosure(ast: Tree, symTbl: Map[String, String], child: Op) extends UDFClosure(symTbl)

  case class FilterUDFClosure(ast: Tree, symTbl: Map[String, String], child: Op) extends UDFClosure(symTbl)
  
  case class FoldUDFClosure(zElem: Tree, sngAst: Tree, uniAst: Tree, symTbl: Map[String, String], child: Op)
    extends UDFClosure(symTbl)

  trait JsonIRGenerator[Ret <: Node] {
    protected def generate: Ret
  }

  def matchConst(c: Constant): Const =
    if (c.tpe == typeOf[Short] || c.tpe == typeOf[Int]) {
      IntConst(c.value.asInstanceOf[Int])
    }
    else if (c.tpe == typeOf[Float]) {
      FloatConst(c.value.asInstanceOf[Float])
    }
    else if (c.tpe == typeOf[Double]) {
      DoubleConst(c.value.asInstanceOf[Double])
    }
    else if (c.tpe == typeOf[String] || c.tpe == typeOf[java.lang.String]) {
      VarCharConst(c.value.asInstanceOf[String])
    }
    else if (c.tpe == typeOf[Char]) {
      CharConst(c.value.asInstanceOf[Char])
    }
    else if (c.tpe == typeOf[Boolean]) {
      BoolConst(c.value.asInstanceOf[Boolean].toString)
    }
    else {
      throw new IllegalArgumentException(s"Constant $c of type ${c.tpe} not supported.")
    }

}