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

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.{higherKinds, implicitConversions}

/** A `DataBag` implementation backed by a Spark `Dataset`. */
class SparkDataset[A: Meta] private[api](@transient private val rep: Dataset[A]) extends DataBag[A] {

  import SparkDataset.{encoderForType, wrap}

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, u: (B, B) => B): B =
    rep.map(x => s(x)).reduce(u) // TODO: handle the empty Dataset case (maybe catch the exception?)

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

  def fetch(): Seq[A] =
    rep.collect()

  // -----------------------------------------------------
  // Conversions
  // -----------------------------------------------------

  def rdd: SparkRDD[A] =
    new SparkRDD(rep.rdd)
}

object SparkDataset {

  import org.apache.spark.sql.Encoder
  import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

  implicit def encoderForType[T: Meta]: Encoder[T] =
    ExpressionEncoder[T]

  implicit def wrap[A: Meta](rep: Dataset[A]): SparkDataset[A] =
    new SparkDataset(rep)

  def apply[A: Meta](implicit spark: SparkSession): SparkDataset[A] =
    spark.emptyDataset[A]

  def apply[A: Meta](seq: Seq[A])(implicit spark: SparkSession): SparkDataset[A] =
    spark.createDataset(seq)
}
