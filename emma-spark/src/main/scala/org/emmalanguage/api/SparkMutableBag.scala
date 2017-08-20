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

import api.Meta.Projections.ctagFor
import api.spark._

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.KeySerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util.UUID

class SparkMutableBag[K: Meta, V: Meta]
(
  init: DataBag[(K, V)]
)(
  implicit spark: SparkSession
) extends MutableBag[K, V] with Serializable {

  import SparkMutableBag.keySerializer

  /* Distrubted state - a (mutable) reference to an (immutable) indexed collection. */
  @transient var si = IndexedRDD(init.as[RDD]).cache()

  def update[M: Meta](ms: DataBag[Group[K, M]])(f: UpdateFunction[M]): DataBag[(K, V)] = {
    // update messages as RDD
    val delta = (si rightOuterJoin ms.map(m => m.key -> m.values).as[RDD])
      .flatMap { case (k, (ov, m)) => f(k, ov, m).map(v => k -> v) }

    si = si.multiputRDD(delta)

    // return the result wrapped in a DataSet
    new SparkRDD(delta)
  }

  def bag(): DataBag[(K, V)] =
    new SparkRDD(si.map(identity[(K, V)]))

  def copy(): MutableBag[K, V] =
    new SparkMutableBag(bag())
}

object SparkMutableBag {
  def apply[K: Meta, V: Meta](
    init: DataBag[(K, V)]
  )(
    implicit spark: SparkSession
  ): MutableBag[K, V] = new SparkMutableBag[K, V](init)

  implicit private def keySerializer[K](implicit m: Meta[K]): KeySerializer[K] = {
    import scala.reflect.runtime.universe._

    val ttag = m

    if (ttag.tpe == typeOf[Long]) IndexedRDD.longSer.asInstanceOf[KeySerializer[K]]
    else if (ttag.tpe == typeOf[String]) IndexedRDD.stringSer.asInstanceOf[KeySerializer[K]]
    else if (ttag.tpe == typeOf[Short]) IndexedRDD.shortSer.asInstanceOf[KeySerializer[K]]
    else if (ttag.tpe == typeOf[Int]) IndexedRDD.intSet.asInstanceOf[KeySerializer[K]]
    else if (ttag.tpe == typeOf[BigInt]) IndexedRDD.bigintSer.asInstanceOf[KeySerializer[K]]
    else if (ttag.tpe == typeOf[UUID]) IndexedRDD.uuidSer.asInstanceOf[KeySerializer[K]]
    else throw new RuntimeException(s"Unsupported Key type ${ttag.tpe}")
  }
}
