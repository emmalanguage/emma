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

import org.apache.spark.sql.SparkSession

class SparkDatasetSpec extends DataBagSpec with SparkAware {

  override type TestBag[A] = SparkDataset[A]
  override type BackendContext = SparkSession

  override val TestBag = SparkDataset
  override val suffix = "spark-dataset"

  override def withBackendContext[T](f: BackendContext => T): T =
    f(defaultSparkSession)

  "broadcast support" in withBackendContext(implicit env => {
    val us = TestBag(400 to 410)
    val bu = spark.SparkNtv.broadcast(us)
    val vs = TestBag(440 to 450)
    val bv = spark.SparkNtv.broadcast(vs)
    val ws = TestBag(480 to 490)
    val bw = spark.SparkNtv.broadcast(ws)
    val xs = TestBag(1 to 50)
    val fn = (x: Int) => {
      val us = spark.SparkNtv.bag(bu)
      val vs = spark.SparkNtv.bag(bv)
      val ws = spark.SparkNtv.bag(bw)
      (us union vs union ws).exists(_ == x * x)
    }
    val rs = xs.withFilter(fn)
    rs.collect() should contain theSameElementsAs Seq(20, 21, 22)
  })
}
