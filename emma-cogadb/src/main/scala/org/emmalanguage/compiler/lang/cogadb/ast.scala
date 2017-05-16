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

/** An abstract syntax tree for CoGaDB plans. */
object ast {

  trait Result

  // ---------------------------------------------------------------------------
  // Operators
  // ---------------------------------------------------------------------------
  sealed trait Node

  //@formatter:off
  sealed trait Op extends Node
  case class Root(child: Op) extends Op
  case class Sort(sortCols: Seq[SortCol], child: Op) extends Op
  case class GroupBy(groupCols: Seq[AttrRef], aggFuncs: Seq[AggFunc], child: Op) extends Op
  case class Selection(predicate: Seq[Predicate], child: Op) extends Op
  case class TableScan(tableName: String, version: Short = 1) extends Op
  case class Projection(attRef: Seq[AttrRef], child: Op) extends Op
  case class MapUdf(mapUdfOutAttr: Seq[MapUdfOutAttr], mapUdfCode: Seq[MapUdfCode], child: Op) extends Op
  case class Join(joinType: String, predicate: Seq[Predicate], lhs: Op, rhs: Op) extends Op
  case class CrossJoin(lhs: Op, rhs: Op) extends Op
  case class Limit(take: Int, child: Op) extends Op

  case class ExportToCsv(filename: String, separator: String, child: Op) extends Op
  case class MaterializeResult(tableName: String, persistOnDisk: Boolean, child: Op) extends Op
  case class ImportFromCsv(tableName: String, filename: String, separator: String, schema: Seq[SchemaAttr]) extends Op
  //@formatter:on

  // ---------------------------------------------------------------------------
  // Predicates
  // ---------------------------------------------------------------------------

  //@formatter:off
  sealed trait Predicate extends Node
  case class And(conj: Seq[Predicate]) extends Predicate
  case class Or(disj: Seq[Predicate]) extends Predicate
  case class ColCol(lhs: AttrRef, rhs: AttrRef, cmp: Comparator) extends Predicate
  case class ColConst(attr: AttrRef, const: Const, cmp: Comparator) extends Predicate
  //@formatter:on

  // ---------------------------------------------------------------------------
  // Leafs
  // ---------------------------------------------------------------------------
  case class SortCol(table: String, col: String, atype: String,
    result: String, version: Short = 1, order: String) extends Node
  case class SchemaAttr(atype: String, aname: String) extends Node
  case class AttrRef(table: String, col: String, result: String, version: Short = 1) extends Node
  case class MapUdfCode(code: String) extends Node
  case class MapUdfOutAttr(attType: String, attName: String, intVarName: String) extends Node
  case class ReduceUdfOutAttr(attType: String, attName: String, intVarName: String) extends Node

  //case class ReduceUdfAttr(attType: String, attName: String, attInitVal: Const) extends Node

  //@formatter:off

  // ---------------------------------------------------------------------------
  // Aggregation Functions
  // ---------------------------------------------------------------------------
  //TODO: Aggfunc should be Node
  sealed trait AggFunc extends Op
  case class AggFuncSimple(aggFunc: String, attrRef: AttrRef, result: String) extends AggFunc
  case class AggFuncReduce(reduceUdf: AggFunc) extends AggFunc
  case class AlgebraicReduceUdf(reduceUdfPayload: Seq[ReduceUdfPayAttrRef],
    reduceUdfOutAttr: Seq[ReduceUdfOutAttr], reduceUdfCode: Seq[ReduceUdfCode], reduceUdfFinalCode: Seq[ReduceUdfCode])
    extends AggFunc
  //TODO: add other possible aggregation functions

  //Ricky
  case class ReduceUdfCode(code: String) extends Node
  case class ReduceUdfOutAttrRef(attType: String, attName: String, intVarName: String) extends Node
  case class ReduceUdfPayAttrRef(attType: String, attName: String, attInitVal: Const) extends Node

  sealed trait Const extends Node {
    type A
    val value: A
  }
  case class IntConst(value: Int) extends Const {
    override type A = Int
  }
  case class FloatConst(value: Float) extends Const {
    override type A = Float
  }
  case class VarCharConst(value: String) extends Const {
    override type A = String
  }
  case class DoubleConst(value: Double) extends Const {
    override type A = Double
  }
  case class CharConst(value: Char) extends Const {
    override type A = Char
  }
  case class DateConst(value: String) extends Const {
    override type A = String
  }
  case class BoolConst(value: String) extends Const {
    override type A = String
  }
  //@formatter:on

  // ---------------------------------------------------------------------------
  // Comparators
  // ---------------------------------------------------------------------------

  //@formatter:off
  sealed trait Comparator extends Node
  case object Equal extends Comparator
  case object Unequal extends Comparator
  case object GreaterThan extends Comparator
  case object GreaterEqual extends Comparator
  case object LessThan extends Comparator
  case object LessEqual extends Comparator
  //@formatter:on
}
