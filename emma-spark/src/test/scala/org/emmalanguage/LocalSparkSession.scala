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

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Manages a local {@link SparkSession} variable, correctly stopping it after each test.
 * Original code taken from the Apache Spark project.
 *
 * @see https://github.com/apache/spark/blob/branch-2.0/core/src/test/scala/org/apache/spark/LocalSparkContext.scala
 */
trait LocalSparkSession extends BeforeAndAfterAll {
  self: Suite =>

  @transient var spark: SparkSession = _
  @transient var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = LocalSparkSession.getOrCreate()
    sc = spark.sparkContext
  }

  override def afterAll(): Unit = {
    LocalSparkSession.stop(spark)
    spark = null
    sc = null
    super.afterAll()
  }
}

object LocalSparkSession {

  def getOrCreate(): SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("emma-spark-test")
    .getOrCreate()

  def stop(spark: SparkSession): Unit = {
    if (spark != null) spark.stop()
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in a `SparkSession` and ensures that it is stopped afterwards. */
  def withSparkSession[T](f: SparkSession => T): T = {
    val spark = getOrCreate()
    try {
      f(spark)
    } finally {
      stop(spark)
    }
  }

  /** Runs `f` by passing in a `SparkContext` and ensures that it is stopped afterwards. */
  def withSparkContext[T](f: SparkContext => T): T = {
    val spark = getOrCreate()
    try {
      f(spark.sparkContext)
    } finally {
      stop(spark)
    }
  }
}
