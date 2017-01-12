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
package compiler.lang

import net.liftweb.json.JValue

package object cogadb {

  /** Fold. */
  def fold[A](alg: Algebra[A])(root: ast.Node): A = root match {

    // -------------------------------------------------------------------------
    // Operators
    // -------------------------------------------------------------------------

    // TODO ...
    case ast.Root(child) =>
      alg.Root(fold(alg)(child))
    case ast.Sort(sortCols, child) =>
      alg.Sort(sortCols.map(fold(alg)), fold(alg)(child))
    case ast.GroupBy(groupCols, aggSpec, child) =>
      alg.GroupBy(groupCols.map(fold(alg)), aggSpec.map(fold(alg)), fold(alg)(child))
    case ast.Selection(predicate, child) =>
      alg.Selection(predicate.map(fold(alg)), fold(alg)(child))
    case ast.TableScan(tableName, version) => alg.TableScan(tableName, version)
    case ast.Projection(attRef, child) => alg.Projection(attRef.map(fold(alg)), fold(alg)(child))
    case ast.MapUdf(mapUdfOutAttr, mapUdfCode, child) =>
      alg.MapUdf(mapUdfOutAttr.map(fold(alg)), mapUdfCode.map(fold(alg)), fold(alg)(child))
    case ast.Join(joinType, predicate, lhs, rhs) =>
      alg.Join(joinType, predicate.map(fold(alg)), fold(alg)(lhs), fold(alg)(rhs))
    case ast.CrossJoin(lhs, rhs) => alg.CrossJoin(fold(alg)(lhs), fold(alg)(rhs))
    case ast.ExportToCsv(filename, separator, child) =>
      alg.ExportToCsv(filename, separator, fold(alg)(child))
    case ast.MaterializeResult(tableName, persistOnDisk, child) =>
      alg.MaterializeResult(tableName, persistOnDisk, fold(alg)(child))
    case ast.ImportFromCsv(tableName, filename, separator, schema) =>
      alg.ImportFromCsv(tableName, filename, separator, schema.map(fold(alg)))

    // -------------------------------------------------------------------------
    // Predicates
    // -------------------------------------------------------------------------

    case ast.And(conj) => alg.And(conj.map(fold(alg)))
    case ast.Or(disj) => alg.Or(disj.map(fold(alg)))
    case ast.ColCol(lhs, rhs, cmp) => alg.ColCol(fold(alg)(lhs), fold(alg)(rhs), fold(alg)(cmp))
    case ast.ColConst(attr, const, cmp) => alg.ColConst(fold(alg)(attr), fold(alg)(const), fold(alg)(cmp))

    // -------------------------------------------------------------------------
    // Leafs
    // -------------------------------------------------------------------------

    case ast.SchemaAttr(atype, aname) => alg.SchemaAttr(atype, aname)

    case ast.AttrRef(table, col, result, version) => alg.AttrRef(table, col, result, version)

    case ast.IntConst(value) => alg.IntConst(value)
    // TODO ...
    case ast.MapUdfCode(code) => alg.MapUdfCode(code)
    case ast.MapUdfOutAttr(attType, attName, intVarName) => alg.MapUdfOutAttr(attType, attName, intVarName)
    case ast.AggSpec(aggFunc, attrRef, resultName) => alg.AggSpec(aggFunc, fold(alg)(attrRef), resultName)
    //case ast.GroupCol(attrRef) => alg.GroupCol(fold(alg)(attrRef))
    case ast.SortCol(table, col, atype, result, version, order) =>
      alg.SortCol(table, col, atype, result, version, order)


    case ast.FloatConst(value) => alg.FloatConst(value)
    case ast.VarCharConst(value) => alg.VarCharConst(value)
    case ast.DoubleConst(value) => alg.DoubleConst(value)
    case ast.CharConst(value) => alg.CharConst(value)
    case ast.DateConst(value) => alg.DateConst(value)
    case ast.BoolConst(value) => alg.BoolConst(value)



    // -------------------------------------------------------------------------
    // Comparators
    // -------------------------------------------------------------------------

    case ast.Equal => alg.Equal
    case ast.Unequal => alg.Unequal

    // TODO ...
    case ast.GreaterThan => alg.GreaterThan
    case ast.GreaterEqual => alg.GreaterEqual
    case ast.LessThan => alg.LessThan
    case ast.LessEqual => alg.LessEqual

    case _ => throw new RuntimeException("Missing match case in fold")
  }

  val asJson: ast.Node => JValue = fold(JSerializer) _
}
