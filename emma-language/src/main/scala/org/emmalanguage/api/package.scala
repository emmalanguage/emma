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

package object api {
  type CSV = io.csv.CSV
  type CSVSupport[A] = io.csv.CSVScalaSupport[A]
  type CSVConverter[A] = io.csv.CSVConverter[A]
  type CSVColumn[A] = io.csv.CSVColumn[A]
  type Parquet = io.parquet.Parquet
  type ParquetSupport[A] = io.parquet.ParquetScalaSupport[A]
  type ParquetConverter[A] = io.parquet.ParquetConverter[A]
  type Meta[A] = scala.reflect.runtime.universe.TypeTag[A]

  val CSV = io.csv.CSV
  val CSVScalaSupport = io.csv.CSVScalaSupport
  val CSVConverter = io.csv.CSVConverter
  val CSVColumn = io.csv.CSVColumn
  val Parquet = io.parquet.Parquet
  val ParquetScalaSupport = io.parquet.ParquetScalaSupport
  val ParquetConverter = io.parquet.ParquetConverter
  val TextSupport = io.text.TextSupport
}
