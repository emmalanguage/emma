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

import org.apache.spark.SparkContext
import org.apache.spark.rdd._

import scala.language.{higherKinds, implicitConversions}

/** A `DataBag` implementation backed by a Spark `RDD`. */
class SparkRDD[A: Meta] private[api](@transient private val rep: RDD[A]) extends DataBag[A] {

  import SparkRDD.wrap

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
    case rdd: SparkRDD[A] => this.rep union rdd.rep
  }

  override def distinct: SparkRDD[A] =
    rep.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  def fetch(): Seq[A] =
    rep.collect()
}

object SparkRDD {

  implicit def wrap[A: Meta](rep: RDD[A]): SparkRDD[A] =
    new SparkRDD(rep)

  def apply[A: Meta]()(implicit sc: SparkContext): SparkRDD[A] =
    sc.emptyRDD[A]

  def apply[A: Meta](seq: Seq[A])(implicit sc: SparkContext): SparkRDD[A] =
    sc.parallelize(seq)
}