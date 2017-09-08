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

import Meta.Projections._
import alg.Alg
import spark.encoderForType
import util.RanHash

import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

import scala.util.hashing.MurmurHash3

/** A `DataBag` implementation backed by a Spark `RDD`. */
class SparkRDD[A: Meta] private[api]
(
  @transient private[emmalanguage] val rep: RDD[A]
)(
  @transient private[emmalanguage] implicit val spark: SparkSession
) extends DataBag[A] {

  import spark.sqlContext.implicits._

  @transient override val m = implicitly[Meta[A]]

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](alg: Alg[A, B]): B =
    rep.map(x => alg.init(x)).fold(alg.zero)(alg.plus)

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): DataBag[B] =
    SparkRDD(rep.map(f))

  override def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B] =
    SparkRDD(rep.flatMap((x: A) => f(x).collect()))

  def withFilter(p: (A) => Boolean): DataBag[A] =
    SparkRDD(rep.filter(p))

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    SparkRDD(rep.groupBy(k).map { case (key, vals) => Group(key, DataBag(vals.toSeq)) })

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case dbag: ScalaSeq[A] => this union SparkRDD(dbag.rep)
    case dbag: SparkRDD[A] => SparkRDD(this.rep union dbag.rep)
    case dbag: SparkDataset[A] => SparkRDD(this.rep union dbag.rep.rdd)
    case _ => throw new IllegalArgumentException(s"Unsupported rhs for `union` of type: ${that.getClass}")
  }

  override def distinct: DataBag[A] =
    SparkRDD(rep.distinct)

  // -----------------------------------------------------
  // Partition-based Ops
  // -----------------------------------------------------

  def sample(k: Int, seed: Long = 5394826801L): Vector[A] = {
    // sample per partition and sorted by partition ID
    val Seq(hd, tl@_*) = rep.zipWithIndex()
      .mapPartitionsWithIndex({ (pid, it) =>
        val sample = Array.fill(k)(Option.empty[A])
        for ((e, i) <- it) {
          if (i >= k) {
            val j = RanHash(seed).at(i).nextLong(i + 1)
            if (j < k) sample(j.toInt) = Some(e)
          } else sample(i.toInt) = Some(e)
        }
        Seq(pid -> sample).toIterator
      }).collect().sortBy(_._1).map(_._2).toSeq

    // merge the sequence of samples and filter None values
    val rs = for {
      Some(v) <- tl.foldLeft(hd)((xs, ys) => for ((x, y) <- xs zip ys) yield y orElse x)
    } yield v

    // convert result to vector
    rs.toVector
  }

  def zipWithIndex(): DataBag[(A, Long)] =
    SparkRDD(rep.zipWithIndex())

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  override def writeCSV(path: String, format: CSV)(
    implicit converter: CSVConverter[A]
  ): Unit = SparkDataset(spark.createDataset(rep)).writeCSV(path, format)

  override def writeText(path: String): Unit =
    SparkDataset(spark.createDataset(rep)).writeText(path)

  def writeParquet(path: String, format: Parquet)(
    implicit converter: ParquetConverter[A]
  ): Unit = SparkDataset(spark.createDataset(rep)).writeParquet(path, format)

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

object SparkRDD extends DataBagCompanion[SparkSession] {

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def empty[A: Meta](
    implicit spark: SparkSession
  ): DataBag[A] = SparkRDD(spark.sparkContext.emptyRDD[A])

  def apply[A: Meta](values: Seq[A])(
    implicit spark: SparkSession
  ): DataBag[A] = SparkRDD(spark.sparkContext.parallelize(values))

  def readText(path: String)(
    implicit spark: SparkSession
  ): DataBag[String] = SparkDataset.readText(path)

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(
    implicit spark: SparkSession
  ): DataBag[A] = SparkDataset.readCSV(path, format)

  def readParquet[A: Meta : ParquetConverter](path: String, format: Parquet)(
    implicit spark: SparkSession
  ): DataBag[A] = SparkDataset.readParquet(path, format)

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  private[api] def apply[A: Meta](
    rep: RDD[A]
  )(implicit s: SparkSession): DataBag[A] = new SparkRDD(rep)

  private[api] def unapply[A: Meta](
    bag: DataBag[A]
  )(implicit spark: SparkSession): Option[RDD[A]] = bag match {
    case bag: SparkRDD[A] => Some(bag.rep)
    case bag: SparkDataset[A] => Some(bag.rep.rdd)
    case _ => Some(spark.sparkContext.parallelize(bag.collect()))
  }
}
