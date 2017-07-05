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

import api.alg.Alg
import compiler.lang.cogadb._
import compiler.lang.cogadb.ast.MapUdf

import runtime.CoGaDB

import scala.language.implicitConversions

/** A `DataBag` implementation backed by a Scala `Seq`. */
class CoGaDBTable[A: Meta] private[emmalanguage]
(
  private[emmalanguage] var rep: ast.Op
)(
  @transient implicit val cogadb: CoGaDB
) extends DataBag[A] {

  @transient override val m = implicitly[Meta[A]]

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  def fold[B: Meta](agg: Alg[A, B]): B =
    ???

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  def map[B: Meta](f: (A) => B): DataBag[B] =
    ???

  def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B] =
    ???

  def withFilter(p: (A) => Boolean): DataBag[A] =
    ???

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    ???

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  def union(that: DataBag[A]): DataBag[A] =
    ???

  def distinct: DataBag[A] =
    ???


  // -----------------------------------------------------
  // Partition-based Ops
  // -----------------------------------------------------

  def sample(k: Int, seed: Long = 5394826801L): Vector[A] =
    ???

  def zipWithIndex(): DataBag[(A, Long)] =
    ???

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit =
    CSVScalaSupport(format).write(path)(collect())

  def writeText(path: String): Unit =
    TextSupport.write(path)(collect() map (_.toString))

  def writeParquet(path: String, format: Parquet)(implicit converter: ParquetConverter[A]): Unit =
    ParquetScalaSupport(format).write(path)(collect())

  def collect(): Seq[A] =
    cogadb.exportSeq(rep)

  // -----------------------------------------------------
  // Helpers
  // -----------------------------------------------------

  private[emmalanguage] def ref(field: String): ast.AttrRef =
    ref(field, field)

  private[emmalanguage] def ref(field: String, alias: String): ast.AttrRef =
    resolve(field, alias)(rep)

  private def resolve(field: String, alias: String)(node: ast.Op): ast.AttrRef =
    node match {
      case ast.ImportFromCsv(tablename, _, _, schema) =>
        schema.collectFirst({
          case ast.SchemaAttr(_, `field`) =>
            ast.AttrRef(tablename.toUpperCase, field, alias)
        }).get
      case ast.TableScan(tablename, version) =>
        ast.AttrRef(tablename, field, alias, version)
      case ast.Projection(attrRef, _) =>
        attrRef.collectFirst({
          case ref@ast.AttrRef(table, col, `field`, version) =>
            ref
        }).get
      case ast.Root(child) =>
        resolve(field, alias)(child)
      case ast.Sort(_, child) =>
        resolve(field, alias)(child)
      case ast.GroupBy(_, _, child) =>
        resolve(field, alias)(child)
      case ast.Selection(_, child) =>
        resolve(field, alias)(child)
      case ast.ExportToCsv(_, _, child) =>
        resolve(field, alias)(child)
      case ast.MaterializeResult(_, _, child) =>
        resolve(field, alias)(child)
      case _ =>
        throw new IllegalArgumentException
    }

  private[emmalanguage] def refTable(): String =
    resolveTable()(rep)

  private def resolveTable()(node: ast.Op): String =
    node match {
      case ast.TableScan(tablename, version) =>
        tablename
      case ast.Root(child) =>
        resolveTable()(child)
      case ast.Sort(_, child) =>
        resolveTable()(child)
      case ast.GroupBy(_, _, child) =>
        resolveTable()(child)
      case ast.Selection(_, child) =>
        resolveTable()(child)
      case ast.ExportToCsv(_, _, child) =>
        resolveTable()(child)
      case ast.MaterializeResult(_, _, child) =>
        resolveTable()(child)
      case ast.Projection(_, child) =>
        resolveTable()(child)
      case MapUdf(_, _, child) =>
        resolveTable()(child)
      case _ =>
        print(node)
        throw new IllegalArgumentException
    }


  private[emmalanguage] def fixMapped()(node: ast.Op): ast.Op =
    node

}

object CoGaDBTable extends DataBagCompanion[CoGaDB] {

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def empty[A: Meta](
    implicit cogadb: CoGaDB
  ): DataBag[A] = CoGaDBTable.apply(Seq.empty[A])

  def apply[A: Meta](values: Seq[A])(
    implicit cogadb: CoGaDB
  ): DataBag[A] = cogadb.importSeq(values)

  def readText(path: String)(
    implicit cogadb: CoGaDB
  ): DataBag[String] = DataBag(TextSupport.read(path).toStream)

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(
    implicit cogadb: CoGaDB
  ): DataBag[A] = DataBag(CSVScalaSupport(format).read(path).toStream)

  def readParquet[A: Meta : ParquetConverter](path: String, format: Parquet)(
    implicit cogadb: CoGaDB
  ): DataBag[A] = DataBag(ParquetScalaSupport(format).read(path).toStream)

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  private implicit def wrap[A: Meta](rep: ast.Op)(implicit cogadb: CoGaDB): DataBag[A] =
    new CoGaDBTable[A](rep)
}
