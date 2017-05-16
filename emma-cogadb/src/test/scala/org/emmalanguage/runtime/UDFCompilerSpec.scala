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
package runtime

import org.emmalanguage.api.CoGaDBTable
import org.emmalanguage.cogadb.CoGaDB
import org.emmalanguage.compiler.lang.cogadb._
import org.emmalanguage.compiler.udf.UDFTransformer
import org.emmalanguage.compiler.udf.common.MapUDFClosure

import org.scalatest._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

class UDFCompilerSpec extends FreeSpec with Matchers with CoGaDBSpec {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)

  "simple map udf"  in withCoGaDB { implicit cogadb: CoGaDB =>
    val A = Seq((1, "foo"), (2, "bar"))

    val simpleA = new CoGaDBTable[(Int,String)](cogadb.importSeq(A))

    val astWithDouble = typecheck(reify {
      () => (t: (Int, String)) => t._1 + 1
    }.tree)

    val act = new CoGaDBTable[(Int,String,Int)](new UDFTransformer(
      MapUDFClosure(astWithDouble, Map[String, String]("t" -> simpleA.refTable), simpleA.rep)).transform
    )

    //val fun1 = "() => (t: (Int, String)) => t._1 + 1"
    //should not work with Node, works with Op
    //val tmp = cogadb.execute(UDFCompiler.compile(scanA, UDFType.Map, Map[String, String]("t" -> "SOURCE0000"), fun1))
    val exp = Seq((1, "foo", 2), (2, "bar", 3))

    act.fetch() should contain theSameElementsAs (exp)

  }

  "simple iteration with map udf" in withCoGaDB { implicit cogadb: CoGaDB =>

    var next = Seq(("foo", 1), ("bar", 2))
    val increment = typecheck(reify {
      () => (t: (String, Int)) => t._2 + 1
    }.tree)

    var i = 1
    while (i < 3) {
      var act = new CoGaDBTable[(String, Int)](cogadb.importSeq(next))

      act = new CoGaDBTable[(String, Int)]({


        val mapped = new UDFTransformer(
          MapUDFClosure(increment, Map[String, String]("t" -> act.refTable()), act.rep)).transform

        val projectedFields = Seq(
          act.ref("_1", "_1"),
          ast.AttrRef("<COMPUTED>", "MAP_UDF_RES_" + i, "MAP_UDF_RES_" + i, 1)
        )

        ast.Projection(projectedFields, mapped)

      })

      next = act.fetch()

      i += 1
    }

    val exp = Seq(("foo", 3), ("bar", 4))

    next should contain theSameElementsAs (exp)

  }

}
