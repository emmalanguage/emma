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
import api.alg._
import api.backend.ComprehensionCombinators
import api.backend.Runtime

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/** Spark backend operators. */
object SparkOps extends ComprehensionCombinators[SparkSession] with Runtime[SparkSession] {

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
      case _ => Some(spark.sparkContext.parallelize(bag.collect()))
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
    xs: DataBag[A], key: A => K, alg: Alg[A, B]
  )(implicit spark: SparkSession): DataBag[Group[K, B]] = {
    // extract rdd
    val rdd = xs match {
      case xs: SparkRDD[A] => xs.rep
      case xs: SparkDataset[A] => xs.rep.rdd
    }
    // do computation
    val res = rdd
      .map(x => key(x) -> alg.init(x))
      .reduceByKey(alg.plus)
      .map(x => Group(x._1, x._2))
    // wrap in DataBag subtype corresponding to the original value
    xs match {
      case _: SparkRDD[A] => SparkRDD.wrap(res)
      case _: SparkDataset[A] => SparkDataset.wrap(spark.createDataset(res))
    }
  }
}
