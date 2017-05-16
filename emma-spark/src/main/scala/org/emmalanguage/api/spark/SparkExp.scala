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
package api.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => fun}

/** Wrapper around the supported subset of the Spark expression API. */
object SparkExp {
  def rootProj(x: String, field: String)(implicit s: SparkSession): Column = {
    import s.implicits._; $"$x.$field"
  }

  def nestProj(x: Column, field: String)(implicit s: SparkSession): Column =
    x.getField(field)

  def rootStruct(names: String*)(vals: Any*): Seq[Column] = {
    assert(vals.size == names.size, "Number of field values and field names for struct must be the same")
    for ((col, name) <- vals zip names) yield fun.lit(col).as(name)
  }

  def nestStruct(names: String*)(vals: Any*): Column = {
    assert(vals.size == names.size, "Number of field values and field names for struct must be the same")
    fun.struct((for ((col, name) <- vals zip names) yield fun.lit(col).as(name)): _*)
  }

  def eq(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) eqNullSafe fun.lit(y)

  def ne(x: Any, y: Any)(implicit s: SparkSession): Column =
    !(fun.lit(x) eqNullSafe y)

  def gt(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) gt fun.lit(y)

  def lt(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) lt fun.lit(y)

  def geq(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) geq fun.lit(y)

  def leq(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) leq fun.lit(y)

  def not(x: Any)(implicit s: SparkSession): Column =
    !fun.lit(x)

  def or(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) or fun.lit(y)

  def and(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) and fun.lit(y)

  def plus(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) plus fun.lit(y)

  def minus(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) minus fun.lit(y)

  def multiply(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) multiply fun.lit(y)

  def divide(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) divide fun.lit(y)

  def mod(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) mod fun.lit(y)

  def startsWith(x: Any, y: Any)(implicit s: SparkSession): Column =
    fun.lit(x) startsWith fun.lit(y)
}
