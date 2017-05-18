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
package api

import runtime.CoGaDB
import compiler.lang.cogadb.ast
import org.emmalanguage.compiler.udf.UDFTransformer
import org.emmalanguage.compiler.udf.common.MapUDFClosure

import org.scalatest._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

/**
 * A spec for the TPC-H Queries for the CoGaDB backend
 * In order to be able to run the tests, the
 * 1) TPC-H reference databases must be installed,
 * as described in the README
 * The script is located in the CoGaDB directory:
 * `utility_scripts/setup_reference_databases.sh`
 * 2) The CoGaDB startup script e.g. `test/resources/cogadb/tpch.coga`
 * must specify the correct path to the database files e.g.
 * `set path_to_database=/home/user/cogadb_reference_dbs/cogadb_reference_databases_v1/tpch_sf1/`
 * but also load the database from the path using following commands:
 * `set table_loader_mode=disk`
 * `loaddatabase`
 */
class TPCHSpec extends FreeSpec with Matchers with CoGaDBSpec {

  case class Lineitem(l_orderkey: Int, l_partkey: Int, l_suppkey: Int, l_linenumber: Int, l_quantity: Double,
    l_extendedprice: Double, l_discount: Double, l_tax: Double, l_returnflag: String, l_linestatus: String,
    l_shipdate: String, l_commitdate: String, l_receiptdate: String, l_shipinstruct: String, l_shipmode: String,
    l_comment: String)

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)


  "TPCH-01" in withCoGaDB { implicit cogadb: CoGaDB =>

    val customUdf = typecheck(reify {
      () => (t: Lineitem) =>
        (t.l_extendedprice * (1 - t.l_discount), t.l_extendedprice * (1 - t.l_discount) * (1 + t.l_tax))
    }.tree)

    val act = new CoGaDBTable[(String, String, Double, Double, Double, Double, Double, Double, Double, Int)](
      ast.Sort(
        Seq(
          ast.SortCol("LINEITEM", "L_RETURNFLAG", "VARCHAR", "L_RETURNFLAG", 1, "ASCENDING"),
          ast.SortCol("LINEITEM", "L_LINESTATUS", "VARCHAR", "L_LINESTATUS", 1, "ASCENDING")
        ),
        ast.GroupBy(
          Seq(
            ast.AttrRef("LINEITEM", "L_RETURNFLAG", "L_RETURNFLAG", 1),
            ast.AttrRef("LINEITEM", "L_LINESTATUS", "L_LINESTATUS", 1)
          ),
          Seq(
            ast.AggFuncSimple("SUM", ast.AttrRef("LINEITEM", "L_QUANTITY", "L_QUANTITY", 1), "SUM_QTY"),
            ast.AggFuncSimple("SUM", ast.AttrRef("LINEITEM", "L_EXTENDEDPRICE", "SUM_BASE_PRICE", 1), "SUM_BASE_PRICE"),
            ast.AggFuncSimple("SUM", ast.AttrRef("<COMPUTED>", "MAP_UDF_RES__1_1", "SUM_DISC_PRICE", 1),
              "SUM_DISC_PRICE"),
            ast.AggFuncSimple("SUM", ast.AttrRef("<COMPUTED>", "MAP_UDF_RES__2_1", "SUM_DISC_PRICE", 1), "SUM_CHARGE"),
            ast.AggFuncSimple("AVG", ast.AttrRef("LINEITEM", "L_QUANTITY", "AVG_QTY", 1), "AVG_QTY"),
            ast.AggFuncSimple("AVG", ast.AttrRef("LINEITEM", "L_EXTENDEDPRICE", "AVG_PRICE", 1), "AVG_PRICE"),
            ast.AggFuncSimple("AVG", ast.AttrRef("LINEITEM", "L_DISCOUNT", "AVG_DISC", 1), "AVG_DISC"),
            ast.AggFuncSimple("COUNT", ast.AttrRef("LINEITEM", "L_DISCOUNT", "COUNT_ORDER", 1), "COUNT_ORDER")
          ),
          new UDFTransformer(
            MapUDFClosure(customUdf, Map[String, String]("t" -> "LINEITEM"),
              ast.Selection(
                Seq(ast.ColConst(ast.AttrRef("LINEITEM", "L_SHIPDATE", "L_SHIPDATE", 1),
                  ast.DateConst("1998-09-02"), ast.LessThan)),
                ast.TableScan("LINEITEM")
              )
            )).transform
        )
      ))


    val exp = Seq(
      ("A", "F", 3.7734107E7, 5.6586554402721E10, 5.3758257074361E10, 5.5909064533988E10, 25.522, 38273.13, 0.05,
        1478493),
      ("N", "F", 991417.0, 1.487504710632E9, 1.413082166525E9, 1.469649205431E9, 25.516, 38284.468, 0.05,
        38854),
      ("N", "O", 7.4429437E7, 1.11631834361214E11, 1.06051846454356E11, 1.10297935081582E11, 25.502, 38249.323, 0.05,
        2918531),
      ("R", "F", 3.7719753E7, 5.6568041383989E10, 5.3741292623516E10, 5.5889618430421E10, 25.506, 38250.855, 0.05,
        1478870)
    )

    act.collect() shouldBe exp

    //act.collect().foreach(println)

  }

  "TPCH-03" in withCoGaDB { implicit cogadb: CoGaDB =>

    val revenueUdf = typecheck(reify {
      () => (t: Lineitem) =>
        t.l_extendedprice * (1 - t.l_discount)
    }.tree)

    val act = new CoGaDBTable[(Int, String, Int, Double)](
      ast.Projection(
        Seq(
          ast.AttrRef("LINEITEM", "L_ORDERKEY", "L_ORDERKEY"),
          ast.AttrRef("<COMPUTED>", "REVENUE", "REVENUE"),
          ast.AttrRef("ORDERS", "O_ORDERDATE", "O_ORDERDATE"),
          ast.AttrRef("ORDERS", "O_SHIPPRIORITY", "O_SHIPPRIORITY")
        ),
        ast.Limit(10,
          ast.Sort(
            Seq(
              ast.SortCol("<COMPUTED>", "REVENUE", "DOUBLE", "REVENUE", 1, "DESCENDING"),
              ast.SortCol("ORDERS", "O_ORDERDATE", "STRING", "O_ORDERDATE", 1, "ASCENDING")
            ),
            ast.GroupBy(
              Seq(
                ast.AttrRef("LINEITEM", "L_ORDERKEY", "L_ORDERKEY"),
                ast.AttrRef("ORDERS", "O_ORDERDATE", "O_ORDERDATE"),
                ast.AttrRef("ORDERS", "O_SHIPPRIORITY", "O_SHIPPRIORITY")
              ),
              Seq(
                ast.AggFuncSimple("SUM", ast.AttrRef("<COMPUTED>", "MAP_UDF_RES_1", "MAP_UDF_RES_1"), "REVENUE")
              ), new UDFTransformer(
                MapUDFClosure(revenueUdf, Map[String, String]("t" -> "LINEITEM"),
                  ast.Join("INNER_JOIN",

                    Seq(
                      ast.ColCol(
                        ast.AttrRef("ORDERS", "O_ORDERKEY", "O_ORDERKEY"),
                        ast.AttrRef("LINEITEM", "L_ORDERKEY", "L_ORDERKEY"),
                        ast.Equal
                      )
                    )
                    , ast.Join("INNER_JOIN",
                      Seq(
                        ast.ColCol(
                          ast.AttrRef("CUSTOMER", "C_CUSTKEY", "C_CUSTKEY"),
                          ast.AttrRef("ORDERS", "O_CUSTKEY", "O_CUSTKEY"),
                          ast.Equal
                        )
                      ), ast.Selection(
                        Seq(
                          ast.And(
                            Seq(
                              ast.ColConst(ast.AttrRef("CUSTOMER", "C_MKTSEGMENT", "C_MKTSEGMENT"),
                                ast.VarCharConst("BUILDING"),
                                ast.Equal
                              )
                            )
                          )
                        ),
                        ast.TableScan("CUSTOMER")
                      ), ast.Selection(
                        Seq(
                          ast.And(
                            Seq(
                              ast.ColConst(ast.AttrRef("ORDERS", "O_ORDERDATE", "O_ORDERDATE"),
                                ast.DateConst("1995-03-15"),
                                ast.LessThan)
                            )
                          )
                        ),
                        ast.TableScan("ORDERS")
                      )
                    ),
                    ast.Selection(
                      Seq(
                        ast.ColConst(ast.AttrRef("LINEITEM", "L_SHIPDATE", "L_SHIPDATE"),
                          ast.DateConst("1995-03-15"),
                          ast.GreaterThan)
                      ),
                      ast.TableScan("LINEITEM")
                    ))
                )).transform
            )
          ))
      )

    )

    val exp = Seq(
      (2456423, "19950305", 0, 406181.016),
      (3459808, "19950304", 0, 405838.711),
      (492164, "19950219", 0, 390324.082),
      (1188320, "19950309", 0, 384537.939),
      (2435712, "19950226", 0, 378673.059),
      (4878020, "19950312", 0, 378376.805),
      (5521732, "19950313", 0, 375153.91),
      (2628192, "19950222", 0, 373133.31),
      (993600, "19950305", 0, 371407.447),
      (2300070, "19950313", 0, 367371.148))

    act.collect() shouldBe exp
  }

  "TPCH-06" in withCoGaDB { implicit cogadb: CoGaDB =>

    val customUdf = typecheck(reify {
      () => (t: Lineitem) =>
        t.l_discount * t.l_extendedprice
    }.tree)

    val act = new CoGaDBTable[(Double)](

      ast.Projection(
        Seq(
          ast.AttrRef("<COMPUTED>", "REVENUE", "REVENUE", 1)
        ),
        ast.GroupBy(
          Seq(),
          Seq(ast.AggFuncSimple("SUM", ast.AttrRef("<COMPUTED>", "MAP_UDF_RES_1", "REVENUE", 1), "REVENUE")),
          new UDFTransformer(
            MapUDFClosure(customUdf, Map[String, String]("t" -> "LINEITEM"),
              ast.Selection(
                Seq(
                  ast.And(Seq(
                    ast.ColConst(
                      ast.AttrRef("LINEITEM", "L_QUANTITY", "L_QUANTITY", 1), ast.IntConst(24), ast.LessThan),
                    ast.ColConst(
                      ast.AttrRef("LINEITEM", "L_DISCOUNT", "L_DISCOUNT", 1), ast.FloatConst(0.07F), ast.LessEqual),
                    ast.ColConst(
                      ast.AttrRef("LINEITEM", "L_DISCOUNT", "L_DISCOUNT", 1), ast.FloatConst(0.05F), ast.GreaterEqual),
                    ast.ColConst(
                      ast.AttrRef("LINEITEM", "L_SHIPDATE", "L_SHIPDATE", 1), ast.DateConst("1995-01-01"),
                      ast.LessThan),
                    ast.ColConst(
                      ast.AttrRef("LINEITEM", "L_SHIPDATE", "L_SHIPDATE", 1), ast.DateConst("1994-01-01"),
                      ast.GreaterEqual)
                  )
                  )
                ),
                ast.TableScan("LINEITEM")
              )
            )).transform

        )
      )
    )

    val exp = Seq(1.2314107809E8)
    act.collect() shouldBe exp

  }


}