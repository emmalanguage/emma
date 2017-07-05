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

class SparkRDDSpec extends DataBagSpec with SparkAware {

  override type TestBag[A] = SparkRDD[A]
  override type BackendContext = SparkSession

  override val TestBag = SparkRDD
  override val suffix = "spark-rdd"

  override def withBackendContext[T](f: BackendContext => T): T =
    withDefaultSparkSession(f)
}
