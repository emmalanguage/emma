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

/** CogaDB AST algebra. */
trait Algebra[A] {

  // -------------------------------------------------------------------------
  // Operators
  // -------------------------------------------------------------------------

  // TODO ...
  def Root(child: A): A
  def Sort(sortCols: Seq[A], child: A): A
  def GroupBy(groupCols: Seq[A], aggSpecs: Seq[A], child: A): A
  def Selection(predicate: Seq[A], child: A): A
  def TableScan(tableName: String, version: Int = 1): A
  def Projection(attRef: Seq[A], child: A): A
  def MapUdf(mapUdfOutAttr: Seq[A], mapUdfCode: Seq[A], child: A): A
  def Join(joinType: String, predicate: Seq[A], lhs: A, rhs: A): A
  def CrossJoin(lhs: A, rhs: A): A

  //Operations
  def ExportToCsv(filename: String, separator: String, child: A): A
  def MaterializeResult(tableName: String, persistOnDisk: Boolean, child: A): A
  def ImportFromCsv(tableName: String, filename: String, separator: String, schema: Seq[A]): A

  //@formatter:on
  // -------------------------------------------------------------------------
  // Predicates
  // -------------------------------------------------------------------------

  //@formatter:off
  def And(conj: Seq[A]): A
  def Or(disj: Seq[A]): A
  def ColCol(lhs: A, rhs: A, cmp: A): A
  def ColConst(attr: A, const: A, cmp: A): A
  //@formatter:on

  // -------------------------------------------------------------------------
  // Leafs
  // -------------------------------------------------------------------------

  def SchemaAttr(atype: String, aname: String): A

  def AttrRef(table: String, col: String, result: String, version: Short): A

  //TODO
  def MapUdfCode(code: String): A
  def MapUdfOutAttr(attType: String, attName: String, intVarName: String): A
  def AggSpec(aggFunc: String, attrRef: A, result: String): A
  //def GroupCol(attrRef: A): A
  def SortCol(table: String, col: String, atype: String, result: String, version: Short = 1, order: String): A


  def IntConst(value: Int): A

  //TODO
  def FloatConst(value: Float): A
  def VarCharConst(value: String): A
  def DoubleConst(value: Double): A
  def CharConst(value: Char): A
  def DateConst(value: String): A
  def BoolConst(value: String): A

  // -------------------------------------------------------------------------
  // Comparators
  // -------------------------------------------------------------------------

  //@formatter:off
  def Equal: A
  def Unequal: A
  def GreaterThan: A
  def GreaterEqual: A
  def LessThan: A
  def LessEqual: A
  //@formatter:on
}
