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

import io.csv.{CSV, CSVConverter}

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.{higherKinds, implicitConversions}

/** A `DataBag` implementation backed by a Spark `Dataset`. */
class SparkDataset[A: Meta] private[api](@transient private val rep: Dataset[A]) extends DataBag[A] {

  import SparkDataset.{encoderForType, wrap}

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, u: (B, B) => B): B =
    try {
      rep.map(x => s(x)).reduce(u)
    } catch {
      case e: UnsupportedOperationException if e.getMessage == "empty collection" => z
      case e: Throwable => throw e
    }

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): SparkDataset[B] =
    rep.map(f)

  override def flatMap[B: Meta](f: (A) => DataBag[B]): SparkDataset[B] =
    rep.flatMap((x: A) => f(x).fetch())

  def withFilter(p: (A) => Boolean): SparkDataset[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): SparkRDD[Group[K, DataBag[A]]] =
    rdd.groupBy(k)

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): SparkDataset[A] = that match {
    case dataset: SparkDataset[A] => this.rep union dataset.rep
  }

  override def distinct: SparkDataset[A] =
    rep.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  override def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit =
    rep.write
      .option("header", format.header)
      .option("delimiter", format.delimiter.toString)
      .option("charset", format.charset.toString)
      .option("quote", format.quote.getOrElse('"').toString)
      .option("escape", format.escape.getOrElse('\\').toString)
      .option("nullValue", format.nullValue)
      .mode("overwrite")
      .csv(path)

  def fetch(): Seq[A] =
    rep.collect()

  // -----------------------------------------------------
  // Conversions
  // -----------------------------------------------------

  private def rdd: SparkRDD[A] =
    new SparkRDD(rep.rdd)(implicitly[Meta[A]], rep.sparkSession)
}

object SparkDataset {

  import org.apache.spark.sql.Encoder
  import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

  implicit def encoderForType[T: Meta]: Encoder[T] =
    ExpressionEncoder[T]

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def apply[A: Meta](implicit spark: SparkSession): SparkDataset[A] =
    spark.emptyDataset[A]

  def apply[A: Meta](seq: Seq[A])(implicit spark: SparkSession): SparkDataset[A] =
    spark.createDataset(seq)

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(implicit spark: SparkSession): SparkDataset[A] =
    spark.read
      .option("header", format.header)
      .option("delimiter", format.delimiter.toString)
      .option("charset", format.charset.toString)
      .option("quote", format.quote.getOrElse('"').toString)
      .option("escape", format.escape.getOrElse('\\').toString)
      .option("comment", format.escape.map(_.toString).getOrElse(null.asInstanceOf[String]))
      .option("nullValue", format.nullValue)
      .schema(encoderForType[A].schema)
      .csv(path)
      .as[A]

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  implicit def wrap[A: Meta](rep: Dataset[A]): SparkDataset[A] =
    new SparkDataset(rep)
}
