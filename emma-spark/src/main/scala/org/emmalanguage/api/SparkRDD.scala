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

import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

import scala.language.{higherKinds, implicitConversions}
import scala.util.hashing.MurmurHash3

/** A `DataBag` implementation backed by a Spark `RDD`. */
class SparkRDD[A: Meta] private[api](@transient private[api] val rep: RDD[A])(implicit spark: SparkSession)
  extends DataBag[A] {

  import spark.sqlContext.implicits._
  import Meta.Projections._

  import SparkRDD.{encoderForType, wrap}

  @transient override val m = implicitly[Meta[A]]

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, u: (B, B) => B): B =
    rep.map(x => s(x)).fold(z)(u)

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): SparkRDD[B] =
    rep.map(f)

  override def flatMap[B: Meta](f: (A) => DataBag[B]): SparkRDD[B] =
    rep.flatMap((x: A) => f(x).fetch())

  def withFilter(p: (A) => Boolean): SparkRDD[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): SparkRDD[Group[K, DataBag[A]]] =
    rep.groupBy(k).map { case (key, vals) => Group(key, DataBag(vals.toSeq)) }

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): SparkRDD[A] = that match {
    case dbag: ScalaSeq[A] => this.rep union SparkRDD(dbag.rep).rep
    case dbag: SparkRDD[A] => this.rep union dbag.rep
    case _ => throw new IllegalArgumentException(s"Unsupported rhs for `union` of type: ${that.getClass}")
  }

  override def distinct: SparkRDD[A] =
    rep.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  override def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit = {
    spark
      .createDataset(rep).write
      .option("header", format.header)
      .option("delimiter", format.delimiter.toString)
      .option("charset", format.charset.toString)
      .option("quote", format.quote.getOrElse('"').toString)
      .option("escape", format.escape.getOrElse('\\').toString)
      .option("nullValue", format.nullValue)
      .mode("overwrite")
      .csv(path)
  }

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
}

object SparkRDD {

  import org.apache.spark.sql.Encoder
  import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

  import Meta.Projections._

  implicit def encoderForType[T: Meta]: Encoder[T] =
    ExpressionEncoder[T]

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def empty[A: Meta](implicit spark: SparkSession): SparkRDD[A] =
    spark.sparkContext.emptyRDD[A]

  def apply[A: Meta](values: Seq[A])(implicit spark: SparkSession): SparkRDD[A] =
    spark.sparkContext.parallelize(values)

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(implicit spark: SparkSession): SparkRDD[A] =
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
      .rdd

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  implicit def wrap[A: Meta](rep: RDD[A])(implicit spark: SparkSession): SparkRDD[A] =
    new SparkRDD(rep)

  // ---------------------------------------------------------------------------
  // ComprehensionCombinators
  // (these should correspond to `compiler.ir.ComprehensionCombinators`)
  // ---------------------------------------------------------------------------

  def cross[A: Meta, B: Meta](
    xs: DataBag[A], ys: DataBag[B]
  )(implicit spark: SparkSession): SparkRDD[(A, B)] = {
    val rddOf = new RDDExtractor(spark)
    (xs, ys) match {
      case (rddOf(xsRdd), rddOf(ysRdd)) => xsRdd cartesian ysRdd
    }
  }

  def equiJoin[A: Meta, B: Meta, K: Meta](
    keyx: A => K, keyy: B => K)(xs: DataBag[A], ys: DataBag[B]
  )(implicit spark: SparkSession): SparkRDD[(A, B)] = {
    val rddOf = new RDDExtractor(spark)
    (xs, ys) match {
      case (rddOf(xsRDD), rddOf(ysRDD)) =>
        (xsRDD.map(extend(keyx)) join ysRDD.map(extend(keyy))).values
    }
  }

  private def extend[X, K](k: X => K): X => (K, X) =
    x => (k(x), x)

  private class RDDExtractor(spark: SparkSession) {
    def unapply[A: Meta](bag: DataBag[A]): Option[RDD[A]] = bag match {
      case bag: SparkRDD[A] => Some(bag.rep)
      case _ => Some(spark.sparkContext.parallelize(bag.fetch()))
    }
  }

}