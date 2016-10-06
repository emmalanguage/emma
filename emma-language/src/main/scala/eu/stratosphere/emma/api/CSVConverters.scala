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
package eu.stratosphere
package emma.api

/** Serialization format for encoding to / decoding from CSV records. */
trait CSVConverters[T] {

  /** Parses and returns the value encoded in `record`. */
  def fromCSV(record: Array[String]): T

  /** Serializes `value` using `sep` as field delimiter. */
  def toCSV(value: T, sep: Char): Array[String]
}

object CSVConverters {

  def apply[T](from: Array[String] => T)
    (to: (T, Char) => Array[String]): CSVConverters[T] = {

    new CSVConverters[T] {

      override def fromCSV(record: Array[String]): T =
        from(record)

      override def toCSV(value: T, sep: Char): Array[String] =
        to(value, sep)
    }
  }
}
