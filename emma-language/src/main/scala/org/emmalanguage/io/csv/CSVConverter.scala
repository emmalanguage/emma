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
package io.csv

import scala.language.experimental.macros

/** Serialization format for encoding to / decoding from CSV records. */
trait CSVConverter[T] {

  /** Parses and returns the value encoded in `record`. */
  def from(record: Array[String]): T

  /** Serializes `value` using `sep` as field delimiter. */
  def to(value: T): Array[String]
}

object CSVConverter {

  def apply[T](_from: Array[String] => T)(_to: T => Array[String]): CSVConverter[T] =
    new CSVConverter[T] {

      override def from(record: Array[String]): T =
        _from(record)

      override def to(value: T): Array[String] =
        _to(value)
    }

  implicit def materializeCSVConverter[T]: CSVConverter[T] =
    macro CSVConverterMacro.materialize[T]
}
