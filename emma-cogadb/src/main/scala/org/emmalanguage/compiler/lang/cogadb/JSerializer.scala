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

import net.liftweb.json._

import scala.language.implicitConversions

object JSerializer extends Algebra[JValue] {

  implicit def boolToJBool(v: Boolean): JBool =
    JBool(v)

  implicit def stringToJString(v: String): JString =
    JString(v)

  implicit def shortToJInt(v: Short): JInt =
    JInt(v.toInt)

  implicit def intToJInt(v: Int): JInt =
    JInt(v)

  implicit def floatToJFloat(v: Float): JDouble =
    JDouble(v)

  implicit def doubleToJDouble(v: Double): JDouble =
    JDouble(v)

  implicit def CharToJChar(v: Char): JString =
    JString(v.toString)

  implicit class JValueExtended(value: JValue) {
    def extractField(childString: String): JValue = {
      if ((value \ childString) != JNothing) {
        value.children.head
      } else {
        throw new IllegalArgumentException
      }
    }
  }


  // -------------------------------------------------------------------------
  // Operators
  // -------------------------------------------------------------------------

  override def Root(child: JValue): JValue =
    JObject(
      JField("QUERY_PLAN",child)
    )

  override def Sort(sortCols: Seq[JValue], child: JValue): JValue =
    JObject(
      JField("OPERATOR_NAME", "SORT_BY"),
      JField("SORT_COLUMNS", JArray(sortCols.toList)),
      JField("LEFT_CHILD",child),
      JField("RIGHT_CHILD",JNull)
    )

  override def GroupBy(groupCols: Seq[JValue], aggFuncs: Seq[JValue], child: JValue): JValue =
    JObject(
      JField("OPERATOR_NAME", "GENERIC_GROUPBY"),
      JField("GROUPING_COLUMNS", JArray(groupCols.toList.map(x => JObject(
        JField("ATTRIBUTE_REFERENCE", x)
      )))),
      JField("AGGREGATION_SPECIFICATION", JArray(aggFuncs.toList)),
      JField("LEFT_CHILD",child),
      JField("RIGHT_CHILD",JNull)
    )

  override def Selection(predicate: Seq[JValue], child: JValue): JValue =
    JObject(
      JField("OPERATOR_NAME", "GENERIC_SELECTION"),
      JField("PREDICATE", JObject(
        JField("PREDICATE_TYPE", "AND_PREDICATE"),
        JField("PREDICATES", JArray(predicate.toList.map(x => JObject(
          JField("PREDICATE", x)
        ))))
      )),
      JField("LEFT_CHILD",child),
      JField("RIGHT_CHILD",JNull)
    )

  override def TableScan(tableName: String, version: Int): JValue =
    JObject(
      JField("OPERATOR_NAME", "TABLE_SCAN"),
      JField("TABLE_NAME", tableName.toUpperCase()),
      JField("VERSION", version)
  )

  override def Projection(attrRef: Seq[JValue], child: JValue): JValue =
    JObject(
      JField("OPERATOR_NAME", "PROJECTION"),
      JField("ATTRIBUTES", JArray(attrRef.toList.map(a => JObject(
        JField("ATTRIBUTE_REFERENCE",a)
      ))

      )),
      JField("LEFT_CHILD",child),
      JField("RIGHT_CHILD",JNull)
    )

  override def MapUdf(mapUdfOutAttr: Seq[JValue], mapUdfCode: Seq[JValue], child: JValue): JValue =
    JObject(
      JField("OPERATOR_NAME", "MAP_UDF"),
      JField("MAP_UDF_OUTPUT_ATTRIBUTES", JArray(mapUdfOutAttr.toList)),
      JField("MAP_UDF_CODE", JArray(mapUdfCode.toList)),
      JField("LEFT_CHILD",child),
      JField("RIGHT_CHILD",JNull)
    )

  // TODO: fix atomic predicate
  override def Join(joinType: String, predicate: Seq[JValue], lhs: JValue, rhs: JValue): JValue =
    JObject(
      JField("OPERATOR_NAME", "GENERIC_JOIN"),
      JField("JOIN_TYPE", joinType),
      JField("PREDICATE", JObject(
        JField("PREDICATE_TYPE", "AND_PREDICATE"),
        JField("PREDICATES", JArray(predicate.toList.map(x => JObject(
          JField("PREDICATE",x)
        ))))
      )),
      JField("LEFT_CHILD", lhs),
      JField("RIGHT_CHILD", rhs)
    )

  override def CrossJoin(lhs: JValue, rhs: JValue): JValue =
    JObject(
      JField("OPERATOR_NAME", "CROSS_JOIN"),
      JField("LEFT_CHILD", lhs),
      JField("RIGHT_CHILD", rhs)
    )

  override def Limit(take: Int, child: JValue): JValue =
    JObject(
      JField("OPERATOR_NAME", "LIMIT"),
      JField("NUMBER_OF_ROWS", take),
      JField("LEFT_CHILD", child),
      JField("RIGHT_CHILD", JNull)
    )

  override def ExportToCsv(filename: String, separator: String, child: JValue): JValue =
    JObject(
      JField("OPERATOR_NAME", "EXPORT_INTO_FILE"),
      JField("PATH_TO_DATA_FILE", filename),
      JField("FIELD_SEPARATOR", separator),
      JField("LEFT_CHILD", child),
      JField("RIGHT_CHILD", JNull)
    )

    override def MaterializeResult(tableName: String, persistOnDisk: Boolean, child: JValue): JValue =
      JObject(
        JField("OPERATOR_NAME", "STORE_TABLE"),
        JField("TABLE_NAME", tableName.toUpperCase),
        JField("PERSIST_TABLE_ON_DISK", persistOnDisk),
        JField("LEFT_CHILD",child),
        JField("RIGHT_CHILD", JNull)
      )

  override def ImportFromCsv(tableName: String, filename: String, separator: String, schema: Seq[JValue]): JValue =
    JObject(
      JField("OPERATOR_NAME", "CREATE_TABLE"),
      JField("TABLE_NAME", tableName.toUpperCase),
      JField("TABLE_SCHEMA", JArray(schema.toList)),
      JField("PATH_TO_DATA_FILE", filename),
      JField("FIELD_SEPARATOR", separator)
    )

  // -------------------------------------------------------------------------
  // Predicates
  // -------------------------------------------------------------------------

  override def And(conj: Seq[JValue]): JValue =
    JObject(
      JField("PREDICATE_TYPE", "AND_PREDICATE"),
      JField("PREDICATES", JArray(conj.toList.map(c => JObject(
        JField("PREDICATE", c)
      ))))
    )

  override def Or(disj: Seq[JValue]): JValue =
    JObject(
      JField("PREDICATE_TYPE", "OR_PREDICATE"),
      JField("PREDICATES", JArray(disj.toList.map(d => JObject(
        JField("PREDICATE", d)
      ))))
    )

  override def ColCol(lhs: JValue, rhs: JValue, cmp: JValue): JValue =
    JObject(
      JField("PREDICATE_TYPE", "COLUMN_COLUMN_PREDICATE"),
      JField("LEFT_HAND_SIDE_ATTRIBUTE_REFERENCE", lhs),
      JField("PREDICATE_COMPARATOR", cmp),
      JField("RIGHT_HAND_SIDE_ATTRIBUTE_REFERENCE", rhs)
    )

  override def ColConst(attr: JValue, const: JValue, cmp: JValue): JValue =
    JObject(
      JField("PREDICATE_TYPE", "COLUMN_CONSTANT_PREDICATE"),
      JField("ATTRIBUTE_REFERENCE", attr),
      JField("PREDICATE_COMPARATOR", cmp),
      JField("CONSTANT", const)
    )

  // -------------------------------------------------------------------------
  // Leafs
  // -------------------------------------------------------------------------


  override def SchemaAttr(atype: String, aname: String): JValue =
    JObject(
      JField("ATTRIBUTE_TYPE", atype),
      JField("ATTRIBUTE_NAME", aname)
    )
  override def MapUdfCode(code: String): JValue =
    code

  override def MapUdfOutAttr(attType: String, attName: String, intVarName: String): JValue =
    JObject(
      JField("ATTRIBUTE_TYPE", attType),
      JField("ATTRIBUTE_NAME", attName),
      JField("INTERNAL_VARIABLE_NAME", intVarName)
    )

  override def AggFuncSimple(aggFunc: String, attrRef: JValue, result: String): JValue =
    JObject(
      JField("AGGREGATION_FUNCTION", aggFunc),
      JField("ATTRIBUTE_REFERENCE", attrRef),
      JField("RESULT_NAME", result)
    )

  override def AggFuncReduce(reduceUdf: JValue): JValue =
    JObject(
      JField("AGGREGATION_FUNCTION", "UDF"),
      JField("REDUCE_UDF", reduceUdf)
    )

  override def AlgebraicReduceUdf(reduceUdfPayAttr: Seq[JValue], reduceUdfOutAttr: Seq[JValue],
    reduceUdfCode: Seq[JValue], reduceUdfFinalCode: Seq[JValue]): JValue =
    JObject(
      JField("REDUCE_UDF_TYPE", "ALGEBRAIC"),
      JField("REDUCE_UDF_PAYLOAD_ATTRIBUTES", JArray(reduceUdfPayAttr.toList)),
      JField("REDUCE_UDF_OUTPUT_ATTRIBUTES", JArray(reduceUdfOutAttr.toList)),
      JField("REDUCE_UDF_CODE", JArray(reduceUdfCode.toList)),
      JField("REDUCE_UDF_FINAL_CODE", JArray(reduceUdfFinalCode.toList))
    )

  override def ReduceUdfCode(code: String): JValue =
    JString(code)

  override def ReduceUdfPayAttrRef(attType: String, attName: String, attInitVal: JValue): JValue =
    JObject(
      JField("ATTRIBUTE_TYPE", attType),
      JField("ATTRIBUTE_NAME", attName),
      JField("ATTRIBUTE_INIT_VALUE", attInitVal.extractField("CONSTANT_VALUE"))
    )

  override def ReduceUdfOutAttr(attType: String, attName: String, attIntVarName: String): JValue =
    JObject(
      JField("ATTRIBUTE_TYPE", attType),
      JField("ATTRIBUTE_NAME", attName)
    )
  def SortCol(table: String, col: String, atype: String, result: String, version: Short, order: String): JValue =
    JObject(
      JField("TABLE_NAME", table),
      JField("COLUMN_NAME", col),
      JField("ATTRIBUTE_TYPE", atype),
      JField("VERSION", version),
      JField("RESULT_NAME", result),
      JField("ORDER", order)
    )

  override def AttrRef(table: String, col: String, result: String, version: Short): JValue =
    JObject(
      JField("TABLE_NAME", table),
      JField("COLUMN_NAME", col),
      JField("VERSION", version),
      JField("RESULT_NAME", result),
      JField("ATTRIBUTE_TYPE","INT")
    )

  def IntConst(value: Int): JValue =
    JObject(
      JField("CONSTANT_VALUE", value.toString),
      JField("CONSTANT_TYPE", "INT")
    )
  def FloatConst(value: Float): JValue =
    JObject(
      JField("CONSTANT_VALUE", value.toString),
      JField("CONSTANT_TYPE", "FLOAT")
    )
  def VarCharConst(value: String): JValue =
    JObject(
      JField("CONSTANT_VALUE", value.toString),
      JField("CONSTANT_TYPE", "VARCHAR")
    )
  def DoubleConst(value: Double): JValue =
    JObject(
      JField("CONSTANT_VALUE", value.toString),
      JField("CONSTANT_TYPE", "DOUBLE")
    )
  def CharConst(value: Char): JValue =
    JObject(
      JField("CONSTANT_VALUE", value.toString),
      JField("CONSTANT_TYPE", "CHAR")
    )
  def DateConst(value: String): JValue =
    JObject(
      JField("CONSTANT_VALUE", value),
      JField("CONSTANT_TYPE", "DATE")
    )
  def BoolConst(value: String): JValue =
    JObject(
      JField("CONSTANT_VALUE", value),
      JField("CONSTANT_TYPE", "BOOLEAN")
    )


  // -------------------------------------------------------------------------
  // Comparators
  // -------------------------------------------------------------------------

  override def Equal: JValue =
    JString("EQUAL")

  override def Unequal: JValue =
    JString("UNEQUAL")

  override def GreaterThan: JValue =
    JString("GREATER_THAN")

  override def GreaterEqual: JValue =
    JString("GREATER_EQUAL")

  override def LessThan: JValue =
    JString("LESS_THAN")

  override def LessEqual: JValue =
    JString("LESS_EQUAL")
}