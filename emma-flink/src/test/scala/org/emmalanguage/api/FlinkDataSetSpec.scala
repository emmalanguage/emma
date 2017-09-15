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

import test.schema.Literature._

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.{ExecutionEnvironment => FlinkEnv}

class FlinkDataSetSpec extends DataBagSpec with FlinkAware {

  override val supportsParquet = false

  override type TestBag[A] = FlinkDataSet[A]
  override type BackendContext = FlinkEnv

  override val TestBag = FlinkDataSet
  override val suffix = "flink"

  override def withBackendContext[T](f: BackendContext => T): T =
    withDefaultFlinkEnv(f)

  // explicitly memoize required TypeInformation instances
  // in practice these lines will be implicitly synthesized by the `emma.onFlink` quotes
  FlinkDataSet.memoizeTypeInfo[Book]
  FlinkDataSet.memoizeTypeInfo[Character]
  FlinkDataSet.memoizeTypeInfo[(Book, Seq[Character])]
  FlinkDataSet.memoizeTypeInfo[(Int, Double)]
  FlinkDataSet.memoizeTypeInfo[(String, String)]
  FlinkDataSet.memoizeTypeInfo[Option[Int]]
  FlinkDataSet.memoizeTypeInfo[Option[Character]]
  FlinkDataSet.memoizeTypeInfo[Option[(Int, Double)]]
  FlinkDataSet.memoizeTypeInfo[List[Int]]
  FlinkDataSet.memoizeTypeInfo[Group[Book, DataBag[Character]]]
  FlinkDataSet.memoizeTypeInfo[(Int, Array[Option[Int]])]
  FlinkDataSet.memoizeTypeInfo[(String, Long)]
  FlinkDataSet.memoizeTypeInfo[DataBagSpec.CSVRecord]

  "broadcast support" in withBackendContext(implicit env => {
    val us = TestBag(400 to 410)
    val vs = TestBag(440 to 450)
    val ws = TestBag(480 to 490)
    val xs = TestBag(1 to 50)
    val fn = (ctx: RuntimeContext) => {
      val us1 = flink.FlinkNtv.bag(us)(ctx)
      val vs1 = flink.FlinkNtv.bag(vs)(ctx)
      val ws1 = flink.FlinkNtv.bag(ws)(ctx)
      (y: Int) => (us1 union vs1 union ws1).exists(_ == y * y)
    }
    val rs = flink.FlinkNtv.filter(fn)(xs)
    val b1 = flink.FlinkNtv.broadcast(rs, us)
    val b2 = flink.FlinkNtv.broadcast(b1, vs)
    val b3 = flink.FlinkNtv.broadcast(b2, ws)
    b3.collect() should contain theSameElementsAs Seq(20, 21, 22)
  })
}
