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

import compiler.lang.cogadb.ast
import org.emmalanguage.api.CoGaDBTable
import org.emmalanguage.cogadb.CoGaDB
import org.emmalanguage.compiler.udf.ReduceUDFGenerator
import org.emmalanguage.compiler.udf.UDFTransformer
import org.emmalanguage.compiler.udf.common.MapUDFClosure
import org.emmalanguage.io.csv.CSV
import org.emmalanguage.io.csv.CSVScalaSupport

import org.scalatest._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

class KMeansSpec extends FreeSpec with Matchers with CoGaDBSpec {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)

  val csv = CSV(delimiter = '|', quote = Some(' '), skipRows = 1)

  "k means example" in withCoGaDB { implicit cogadb: CoGaDB =>

    val A = CSVScalaSupport[(Double, Double)](csv).read(getClass.getResource("/kmeans_points.csv").getPath).toStream
    val B = CSVScalaSupport[(Int, Double, Double)](csv)
      .read(getClass.getResource("/kmeans_centroids.csv").getPath).toStream


    val points = new CoGaDBTable[(Double, Double)](cogadb.importSeq(A))
    val centroids = new CoGaDBTable[(Int, Double, Double)](cogadb.importSeq(B))

    val distance = typecheck(reify {
      () => (pcs: (Double, Double, Int, Double, Double)) =>
        scala.math.sqrt(scala.math.pow(pcs._1 - pcs._3, 2) + scala.math.pow(pcs._2 - pcs._4, 2))
    }.tree)

    val cross = new CoGaDBTable[(Double, Double, Int, Double, Double)]({
      ast.CrossJoin(points.rep, centroids.rep)
    }).fetch

    val crossed = new CoGaDBTable[(Double, Double, Int, Double, Double)](cogadb.importSeq(cross))

    val map = new CoGaDBTable[(Double, Double, Int, Double, Double)](new UDFTransformer(
      MapUDFClosure(distance, Map[String, String]("pcs" -> crossed.refTable), crossed.rep)).transform
    ).fetch

    val mapped = new CoGaDBTable[(Double,Double,Int,Double,Double)](cogadb.importSeq(map))

    val symbolTable = Map[String, String](
      "p" -> mapped.refTable())

    val z = typecheck(reify {
      4000000000.0
    }.tree)

    val sngAst = typecheck(reify {
      () => (p: (Double, Double, Int, Double, Double)) =>
        if (p._1 > 0) {
          p._2
        }
        else {
          p._1
        }
    }.tree)

    val uniAst = typecheck(reify {
      () => (x: Double, y: Double) => x + y
    }.tree)

    val actual = new CoGaDBTable[(Int,Int)](
      ast.GroupBy(Seq(mapped.ref("_1"),mapped.ref("_2")),
        Seq(
          new ReduceUDFGenerator(z, sngAst, uniAst, symbolTable, mapped.rep).generate
        ),mapped.rep)
      )
    /*val reduced = new CoGaDBTable[(Double, Double, Int, Double, Double)](new UDFTransformer(
      MapUDFClosure(distance, Map[String, String]("pcs" -> crossed.refTable), crossed.rep)).transform
    )*/

    //val act = new CoGaDBTable[]()

    actual.fetch().foreach(println)
    //val exp = Seq((1, "foo", 2), (2, "bar", 3))

    //act.fetch() should contain theSameElementsAs (exp)

  }

}
