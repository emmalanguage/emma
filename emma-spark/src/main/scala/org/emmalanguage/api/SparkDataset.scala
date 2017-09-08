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

import alg.Alg
import spark.encoderForType

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession

import scala.util.hashing.MurmurHash3

/** A `DataBag` implementation backed by a Spark `Dataset`. */
class SparkDataset[A: Meta] private[api](@transient private[emmalanguage] val rep: Dataset[A]) extends DataBag[A] {

  import rep.sparkSession.sqlContext.implicits._

  @transient override val m = implicitly[Meta[A]]
  private[emmalanguage] implicit def spark = this.rep.sparkSession

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](alg: Alg[A, B]): B =
    SparkRDD(rep.rdd).fold(alg)

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): DataBag[B] =
    SparkRDD(rep.rdd).map(f)

  override def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B] =
    SparkRDD(rep.rdd).flatMap(f)

  def withFilter(p: (A) => Boolean): DataBag[A] =
    SparkRDD(rep.rdd).withFilter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    SparkRDD(rep.rdd).groupBy(k)

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case dbag: ScalaSeq[A] => this union SparkDataset(dbag.rep)
    case dbag: SparkRDD[A] => SparkRDD(this.rep.rdd union dbag.rep)
    case dbag: SparkDataset[A] => SparkDataset(this.rep union dbag.rep)
    case _ => throw new IllegalArgumentException(s"Unsupported rhs for `union` of type: ${that.getClass}")
  }

  override def distinct: DataBag[A] =
    SparkDataset(rep.distinct)

  // -----------------------------------------------------
  // Partition-based Ops
  // -----------------------------------------------------

  def sample(k: Int, seed: Long = 5394826801L): Vector[A] =
    SparkRDD(rep.rdd).sample(k, seed)

  def zipWithIndex(): DataBag[(A, Long)] =
    SparkRDD(rep.rdd).zipWithIndex()

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  override def writeCSV(path: String, format: CSV)(
    implicit converter: CSVConverter[A]
  ): Unit = rep.write
    .option("header", format.header)
    .option("delimiter", format.delimiter.toString)
    .option("charset", format.charset.toString)
    .option("quote", format.quote.getOrElse('"').toString)
    .option("escape", format.escape.getOrElse('\\').toString)
    .option("nullValue", format.nullValue)
    .mode("overwrite").csv(path)

  override def writeText(path: String): Unit =
    rep.write.text(path)

  def writeParquet(path: String, format: Parquet)(
    implicit converter: ParquetConverter[A]
  ): Unit = rep.write
    .option("binaryAsString", format.binaryAsString)
    .option("int96AsTimestamp", format.int96AsTimestamp)
    .option("cacheMetadata", format.cacheMetadata)
    .option("codec", format.codec.toString)
    .mode("overwrite").parquet(path)

  def collect(): Seq[A] =
    collected

  private lazy val collected: Seq[A] =
    rep.collect()

  // -----------------------------------------------------
  // equals, hashCode and toString
  // -----------------------------------------------------

  override def equals(o: Any): Boolean =
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
}

object SparkDataset extends DataBagCompanion[SparkSession] {

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def empty[A: Meta](
    implicit spark: SparkSession
  ): DataBag[A] = SparkDataset(spark.emptyDataset[A])

  def apply[A: Meta](values: Seq[A])(
    implicit spark: SparkSession
  ): DataBag[A] = SparkDataset(spark.createDataset(values))

  def readText(path: String)(
    implicit spark: SparkSession
  ): DataBag[String] = SparkDataset(spark.read.textFile(path))

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(
    implicit spark: SparkSession
  ): DataBag[A] = SparkDataset(spark.read
    .option("header", format.header)
    .option("delimiter", format.delimiter.toString)
    .option("charset", format.charset.toString)
    .option("quote", format.quote.getOrElse('"').toString)
    .option("escape", format.escape.getOrElse('\\').toString)
    .option("comment", format.escape.map(_.toString).orNull)
    .option("nullValue", format.nullValue)
    .schema(implicitly[Encoder[A]].schema)
    .csv(path).as[A])

  def readParquet[A: Meta : ParquetConverter](path: String, format: Parquet)(
    implicit spark: SparkSession
  ): DataBag[A] = SparkDataset(spark.read
    .option("binaryAsString", format.binaryAsString)
    .option("int96AsTimestamp", format.int96AsTimestamp)
    .option("cacheMetadata", format.cacheMetadata)
    .option("codec", format.codec.toString)
    .schema(implicitly[Encoder[A]].schema)
    .parquet(path).as[A])

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  private[api] def apply[A: Meta](
    rep: Dataset[A]
  )(implicit s: SparkSession): DataBag[A] = new SparkDataset(rep)

  private[api] def unapply[A: Meta](
    bag: DataBag[A]
  )(implicit spark: SparkSession): Option[Dataset[A]] = bag match {
    case bag: SparkDataset[A] => Some(bag.rep)
    case bag: SparkRDD[A] => Some(spark.createDataset(bag.rep))
    case _ => Some(spark.createDataset(bag.collect()))
  }
}
