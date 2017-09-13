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

  "broadcast map" in withBackendContext(implicit env => {
    val xs = FlinkDataSet(400 to 600)
    val ys = FlinkDataSet(1 to 50)
    val fn = (ctx: RuntimeContext) => {
      val xs1 = flink.FlinkNtv.bag(ys)(ctx)
      (y: Int) => xs1.exists(_ == y * y)
    }
    val zs1 = flink.FlinkNtv.map(fn)(xs)
    val zs = flink.FlinkNtv.broadcast(zs1, ys)
    print(zs.collect())
  })
}
