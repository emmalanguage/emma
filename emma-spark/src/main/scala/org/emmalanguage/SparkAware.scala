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

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

trait SparkAware {

  Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.WARN)

  protected trait SparkConfig {
    val appName: String
    val master: String
    val warehouseDir: String
  }

  protected val defaultSparkConfig = new SparkConfig {
    val appName = this.getClass.getSimpleName
    val master = "local[*]"
    val warehouseDir = Paths.get(sys.props("java.io.tmpdir"), "spark-warehouse").toUri.toString
  }

  protected lazy val defaultSparkSession =
    sparkSession(defaultSparkConfig)

  protected def sparkSession(c: SparkConfig): SparkSession = SparkSession.builder()
    .appName(c.appName)
    .master(c.master)
    .config("spark.sql.warehouse.dir", c.warehouseDir)
    .getOrCreate()

  protected def withDefaultSparkSession[T](f: SparkSession => T): T =
    f(defaultSparkSession)
}
