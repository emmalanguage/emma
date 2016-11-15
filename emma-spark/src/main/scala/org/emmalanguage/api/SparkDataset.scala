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
import scala.util.hashing.MurmurHash3

/** A `DataBag` implementation backed by a Spark `Dataset`. */
class SparkDataset[A: Meta] private[api](@transient private[api] val rep: Dataset[A]) extends DataBag[A] {

  import SparkDataset.{encoderForType, wrap}
  import rep.sparkSession.sqlContext.implicits._

  @transient override val m = implicitly[Meta[A]]
  private implicit def spark = this.rep.sparkSession

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

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case dbag: ScalaSeq[A] => this.rep union SparkDataset(dbag.rep).rep
    case dbag: SparkRDD[A] => this.rep union dbag.rep.toDS()
    case dbag: SparkDataset[A] => this.rep union dbag.rep
    case _ => that union this
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

  def fetch(): Seq[A] = collect

  private lazy val collect: Seq[A] =
    rep.collect()

  // -----------------------------------------------------
  // equals and hashCode
  // -----------------------------------------------------

  override def equals(o: Any) =
    super.equals(o)

  override def hashCode(): Int = {
    val (a, b, c, n) = rep
      .mapPartitions(it => {
        var a, b, n = 0
        var c = 1
        it foreach { x =>
          val h = x.##
          a += h
          b ^= h
          if (h != 0) c *= h
          n += 1
        }
        Some((a, b, c, n)).iterator
      })
      .collect()
      .fold((0, 0, 1, 0))((x, r) => (x, r) match {
        case ((a1, b1, c1, n1), (a2, b2, c2, n2)) => (a1 + a2, b1 ^ b2, c1 * c2, n1 + n2)
      })

    var h = MurmurHash3.traversableSeed
    h = MurmurHash3.mix(h, a)
    h = MurmurHash3.mix(h, b)
    h = MurmurHash3.mixLast(h, c)
    MurmurHash3.finalizeHash(h, n)
  }

  // -----------------------------------------------------
  // Conversions
  // -----------------------------------------------------

  private def rdd: SparkRDD[A] =
    new SparkRDD(rep.rdd)(implicitly[Meta[A]], rep.sparkSession)
}

object SparkDataset {

  import org.apache.spark.sql.Encoder
  import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

  import Meta.Projections._

  implicit def encoderForType[T: Meta]: Encoder[T] =
    ExpressionEncoder[T]

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def empty[A: Meta](implicit spark: SparkSession): SparkDataset[A] =
    spark.emptyDataset[A]

  def apply[A: Meta](values: Seq[A])(implicit spark: SparkSession): SparkDataset[A] =
    spark.createDataset(values)

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
