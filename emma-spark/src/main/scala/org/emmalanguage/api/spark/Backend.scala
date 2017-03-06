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
package api.spark

import api._
import api.backend.ComprehensionCombinators
import api.backend.Runtime

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.language.higherKinds

/**
 * IR nodes added by backend-agnostic transformations.
 *
 * Do not use those directly unless you want to hardcode physical execution aspects such as
 * join order and caching you know exactly what you are doing.
 */
object Backend extends ComprehensionCombinators[SparkSession] with Runtime[SparkSession] {

  import Meta.Projections._
  import SparkRDD.wrap

  implicit def encoderForType[T: Meta]: Encoder[T] =
    ExpressionEncoder[T]

  //----------------------------------------------------------------------------
  // ComprehensionCombinators
  //----------------------------------------------------------------------------

  /** Dummy `cross` node. */
  def cross[A: Meta, B: Meta](
    xs: DataBag[A], ys: DataBag[B]
  )(implicit spark: SparkSession): DataBag[(A, B)] = {
    val rddOf = new RDDExtractor(spark)
    (xs, ys) match {
      case (rddOf(xsRdd), rddOf(ysRdd)) => xsRdd cartesian ysRdd
    }
  }

  /** Dummy `equiJoin` node. */
  def equiJoin[A: Meta, B: Meta, K: Meta](
    kx: A => K, ky: B => K)(xs: DataBag[A], ys: DataBag[B]
  )(implicit spark: SparkSession): DataBag[(A, B)] = {
    val rddOf = new RDDExtractor(spark)
    (xs, ys) match {
      case (rddOf(xsRDD), rddOf(ysRDD)) =>
        (xsRDD.map(extend(kx)) join ysRDD.map(extend(ky))).values
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

  //----------------------------------------------------------------------------
  // Runtime
  //----------------------------------------------------------------------------

  /** Implement the underlying logical semantics only (identity function). */
  def cache[A: Meta](xs: DataBag[A])(implicit spark: SparkSession): DataBag[A] =
    xs match {
      case xs: SparkRDD[A] => xs.rep.cache()
      case xs: SparkDataset[A] => xs.rep.rdd.cache()
      case _ => xs
    }

  /** Fuse a groupBy and a subsequent fold into a single operator. */
  def foldGroup[A: Meta, B: Meta, K: Meta](
    xs: DataBag[A], key: A => K, sng: A => B, uni: (B, B) => B
  )(implicit spark: SparkSession): DataBag[(K, B)] = xs match {
    case xs: SparkRDD[A] =>
      SparkRDD.wrap(xs.rep.map(x => key(x) -> sng(x)).reduceByKey(uni))
    case xs: SparkDataset[A] =>
      SparkDataset.wrap(spark.createDataset(xs.rep.rdd.map(x => key(x) -> sng(x)).reduceByKey(uni)))
  }
}
