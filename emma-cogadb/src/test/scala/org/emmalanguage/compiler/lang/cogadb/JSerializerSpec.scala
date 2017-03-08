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
package compiler.lang.cogadb

import cogadb.CoGaDB

import net.liftweb.json._
import org.junit.runner.RunWith
import org.scalatest.FreeSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

import scala.language.implicitConversions

import java.io.FileNotFoundException

/** A spec for the [[CoGaDB]] runtime. */
@RunWith(classOf[JUnitRunner])
class JSerializerSpec extends FreeSpec with Matchers {

  "TPC-H Q01" - {

    // TODO: construct and issue TPC-H Query Q01 using CoGaDB AST nodes

    //predicate list
    val predicates = ast.And(Seq(
      ast.ColCol(
        lhs = ast.AttrRef("lineitem", "order_id", "l_order_id"),
        rhs = ast.AttrRef("order", "id", "o_order_id"),
        cmp = ast.Equal
      ),
      ast.ColConst(
        attr = ast.AttrRef("order", "amount", "amount"),
        const = ast.IntConst(500),
        cmp = ast.Unequal
      )
    ))

    //tablescan operator
    val customerScan = ast.Root(ast.TableScan("CUSTOMER"))

    val customerScanSimple = ast.TableScan("CUSTOMER")

    //Aggspec
    val aggSpec = ast.AggSpec("SUM", ast.AttrRef("ORDER", "AMOUNT", "AMOUNT"), "REVENUE")

    //group by
    val attrRefSeq = Seq(ast.AttrRef("LINEITEM","L_ORDERKEY","L_ORDERKEY"),
                        ast.AttrRef("ORDERS","O_ORDERDATE","O_ORDERDATE"),
      ast.AttrRef("ORDERS","O_SHIPPRIORITY","O_SHIPPRIORITY"))

    val groupBy = ast.GroupBy(attrRefSeq, Seq(aggSpec), customerScanSimple)

    //sort by
    val sortBy = ast.Root(ast.Sort(Seq(ast.SortCol("<COMPUTED>","REVENUE","DOUBLE","REVENUE",1,"DESCENDING"),
      ast.SortCol("ORDERS","O_ORDERDATE","DATE","O_ORDERDATE",1,"ASCENDING")), customerScanSimple))

    //join

    val joinPred = ast.And(Seq(
      ast.ColCol(
        lhs = ast.AttrRef("SUPPLIER","S_SUPPKEY","S_SUPPKEY"),
        rhs = ast.AttrRef("PARTSUPP","PS_SUPPKEY","PS_SUPPKEY"),
        cmp = ast.Equal
      )
    ))
    val partSuppScan = ast.TableScan("PARTSUPP")
    val suppScan = ast.TableScan("SUPPLIER")

    val join = ast.Root(ast.Join("INNER_JOIN",Seq(joinPred),suppScan,partSuppScan))


    //export to csv
    val export = ast.Root(ast.ExportToCsv("filename.csv", ",", customerScanSimple))

    "predicates" in {
      val exp = parse(readResourceFile("/test.json"))
      val act = fold(JSerializer)(predicates)
      println(exp)
      println(act)
      act shouldEqual exp
    }

    "table scan" in{

      val exp = parse(readResourceFile("/tablescan.json"))
      val act = fold(JSerializer)(customerScan)

      println(exp)
      println(act)
      act shouldEqual exp
    }

    "aggregation specification" in{

      val exp = parse(readResourceFile("/aggspec.json"))
      val act = fold(JSerializer)(aggSpec)

      println(exp)
      println(act)
      act shouldEqual exp
    }

    "group by" in{
      val exp = parse(readResourceFile("/groupby.json"))
      val act = fold(JSerializer)(groupBy)

      println(exp)
      println(act)
      act shouldEqual exp
    }

    "sort by" in{
      val exp = parse(readResourceFile("/sortby.json"))
      val act = fold(JSerializer)(sortBy)

      println(exp)
      println(act)
      act shouldEqual exp
    }

    "join" in{
      val exp = parse(readResourceFile("/join.json"))
      val act = fold(JSerializer)(join)
      println(exp)
      println(act)
      act shouldEqual exp
    }

    "export" in {
      val exp = parse(readResourceFile("/export.json"))
      val act = fold(JSerializer)(export)
      println(exp)
      println(act)
      act shouldEqual exp
    }


  }

  private def readResourceFile(p: String): String =
    Option(getClass.getResourceAsStream(p))
      .map(scala.io.Source.fromInputStream)
      .map(_.getLines.toList.mkString("\n"))
      .getOrElse(throw new FileNotFoundException(p))
}