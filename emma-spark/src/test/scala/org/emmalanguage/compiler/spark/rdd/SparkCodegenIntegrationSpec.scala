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

import api._
import api.spark._
import compiler.BaseCodegenIntegrationSpec
import compiler.RuntimeCompiler
import compiler.SparkCompiler

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class SparkCodegenIntegrationSpec extends BaseCodegenIntegrationSpec with SparkAware {
  override val compiler = new RuntimeCompiler with SparkCompiler

  import compiler._
  import u.reify

  type Env = SparkSession

  override lazy val Env = api.Type[org.apache.spark.sql.SparkSession]
  override lazy val env = defaultSparkSession

  override lazy val backendPipeline: u.Tree => u.Tree =
    SparkBackend.transform

  // --------------------------------------------------------------------------
  // Distributed collection conversion
  // --------------------------------------------------------------------------

  "Convert from/to a Spark RDD" in {
    implicit val e: Env = env
    verify(reify {
      val xs = DataBag(1 to 1000).withFilter(_ > 800)
      val ys = xs.as[RDD].filter(_ < 200)
      val zs = DataBag.from(ys)
      zs.size
    })
  }
}
