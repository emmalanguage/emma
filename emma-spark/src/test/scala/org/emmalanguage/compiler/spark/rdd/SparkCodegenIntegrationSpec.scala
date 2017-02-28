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
package compiler.spark.rdd

import api.SparkMutableBag
import api.SparkRDD
import compiler.BaseCodegenIntegrationSpec

class SparkCodegenIntegrationSpec extends BaseCodegenIntegrationSpec {

  import compiler._

  lazy val sparkRDD = api.Sym[SparkRDD.type].asModule
  lazy val sparkMutableBag = api.Sym[SparkMutableBag.type].asModule

  override lazy val backendPipeline: u.Tree => compiler.u.Tree =
    Function.chain(Seq(
      Comprehension.desugar(API.bagSymbol),
      Backend.translateToDataflows(sparkRDD),
      Core.refineModules(Map(MutableBagAPI.module -> sparkMutableBag)),
      addContext
    ))

  private lazy val addContext: u.Tree => u.Tree = tree => {
    import u._
    q"""
        implicit val ctx = _root_.org.emmalanguage.LocalSparkSession.getOrCreate()
        $tree
    """
  }
}
