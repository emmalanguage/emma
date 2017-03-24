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

import cogadb.CoGaDB
import compiler.lang.cogadb._
import io.csv._
import io.parquet._
import io.text._

import scala.language.higherKinds
import scala.language.implicitConversions

/** A `DataBag` implementation backed by a Scala `Seq`. */
class CoGaDBTable[A: Meta] private[emmalanguage]
(
  private[emmalanguage] val rep: ast.Op
)(
  @transient implicit val cogadb: CoGaDB
) extends DataBag[A] {

  import Meta.Projections._

  @transient override val m = implicitly[Meta[A]]

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, p: (B, B) => B): B =
    ???

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): DataBag[B] =
    ???

  override def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B] =
    ???

  def withFilter(p: (A) => Boolean): DataBag[A] =
    ???

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    ???

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] =
    ???

  override def distinct: DataBag[A] =
    ???

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit =
    CSVScalaSupport(format).write(path)(fetch())

  def writeText(path: String): Unit =
    TextSupport.write(path)(fetch() map (_.toString))

  def writeParquet(path: String, format: Parquet)(implicit converter: ParquetConverter[A]): Unit =
    ParquetScalaSupport(format).write(path)(fetch())

  override def fetch(): Seq[A] =
    cogadb.exportSeq(rep)
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
