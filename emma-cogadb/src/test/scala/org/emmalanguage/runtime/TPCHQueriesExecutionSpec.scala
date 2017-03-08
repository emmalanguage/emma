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

import api._
import cogadb.CoGaDB
import compiler.lang.cogadb.ast
import test.util._

import org.scalatest._

import java.io.File
import java.nio.file.Paths

class TPCHQueriesExecutionSpec extends FreeSpec with Matchers with BeforeAndAfter {

  val dir = "/cogadb"
  val path = tempPath("/cogadb")

  implicit var cogadb: CoGaDB = null

  before {
    new File(path).mkdirs()
    //val coGaDBPath = Paths.get(Option(System.getenv("COGADB_HOME")) getOrElse "/tmp/cogadb")
    //val coGaDBPath = Paths.get("/home/harry/falcon/debug_build_2/")
    val coGaDBPath = Paths.get("/home/harry/falcontest/falcon/build/")
    val configPath = Paths.get(materializeResource(s"$dir/tpch.coga"))

    cogadb = CoGaDB(coGaDBPath, configPath)
  }

  after {
    //deleteRecursive(new File(path))
    cogadb.destroy()
  }

  "read CUSTOMER" ignore {
    val plan = ast.TableScan("CUSTOMER")
    //cogadb.materialize(plan)
  }

  "sort CUSTOMER" ignore {
    val plan = ast.Sort(Seq(
      ast.SortCol("CUSTOMER", "C_CUSTKEY", "INT", "C_CUSTKEY", 1, "ASCENDING")),
      ast.TableScan("CUSTOMER"))

    //cogadb.materialize(plan)
  }

  "filter CUSTOMER" ignore {
    val plan =
      ast.Selection(
        Seq(ast.And(Seq(
          ast.ColConst(
            ast.AttrRef("CUSTOMER", "CUSTKEY", "CUSTKEY", 1),
            ast.IntConst(500),
            ast.LessEqual)))),
        ast.TableScan("CUSTOMER"))

    //cogadb.materialize(plan)
  }


  "create A" ignore {
    val schema = Seq(
      ast.SchemaAttr("INT", "id"),
      ast.SchemaAttr("VARCHAR", "name"))

    //FIXME: @harrygav
    val plan = ast.ImportFromCsv("A", "/home/haros/Desktop/emmaToCoGaDB/sample_tables/A.csv", ",", schema)

    //cogadb.materialize(plan)
    //cogadb.executeGeneral("import_csv_file","dataflow0000 /home/haros/Desktop/emmaToCoGaDB/sample_tables/A.csv")

  }

  "write" ignore {
    val x = Seq(1)
    val scan = cogadb.importSeq(x)

    //scan shouldBe
  }

  "create and fetch pair" in {
    val A = Seq((1, "foo"), (2, "bar"))

    //load seq in cogadb
    val scanA = cogadb.importSeq[(Int,String)](A)

    //val written = cogadb.apply(scanA)

    val res = cogadb.exportSeq[(Int,String)](scanA)
    val exp = Seq((1, "foo"), (2, "bar"))

    res shouldBe exp
  }

  "create and fetch pair of doubles" in {
    val A = Seq((1.1, 2.1), (2.1, 3.1))

    //load seq in cogadb
    val scanA = cogadb.importSeq[(Double,Double)](A)

    //val written = cogadb.apply(scanA)

    val res = cogadb.exportSeq[(Double,Double)](scanA)
    val exp = Seq((1.1, 2.1), (2.1, 3.1))

    res shouldBe exp
  }

  "cross join A and B" in {
    val A = Seq((1, 10), (2, 20))
    val B = Seq((1, 30), (2, 40))

    val scanA = cogadb.importSeq[(Int, Int)](A)
    val scanB = cogadb.importSeq[(Int, Int)](B)

    //print(ast.CrossJoin(scanA, scanB))
    val res = cogadb.exportSeq[(Int, Int, Int, Int)](ast.CrossJoin(scanA, scanB))

    //print(res)

    //val res = cogadb.importSeq[(Int, Int, Int, Int)](crossed)

    //println(res)
    /*val res = cogadb.exportSeq[(Int, String, Double)](
      cogadb.
        CrossJoin(
        cogadb.importSeq(A),
        cogadb.importSeq(B)))*/
    val exp = for (a <- A; b <- B) yield (a._1, a._2, b._1, b._2)

    res shouldBe exp
  }

  "create and fetch empty" in {
    CoGaDBTable.empty[Int].fetch() shouldBe Seq.empty[Int]
  }
}