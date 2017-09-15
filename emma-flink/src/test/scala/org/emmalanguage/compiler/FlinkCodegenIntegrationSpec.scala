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
package compiler

import api._
import api.flink._

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.createTypeInformation

class FlinkCodegenIntegrationSpec extends BaseCodegenIntegrationSpec
  with FlinkCompilerAware
  with FlinkAware {

  import compiler._

  def withBackendContext[T](f: Env => T): T =
    withDefaultFlinkEnv(f)

  // FIXME: no idea why, but all tests require TypeInformation[(Int, Int, Int, Int)]
  FlinkDataSet.memoizeTypeInfo[(Int, Int, Int, Int)]

  // --------------------------------------------------------------------------
  // Distributed collection conversion
  // --------------------------------------------------------------------------

  "Convert from/to a Flink DataSet" in withBackendContext(implicit env => {
    verify(u.reify {
      val xs = DataBag(1 to 1000).withFilter(_ > 800)
      val ys = xs.as[DataSet].filter(_ < 200)
      val zs = DataBag.from(ys)
      zs.size
    })
  })
}
