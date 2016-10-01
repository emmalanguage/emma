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
package eu.stratosphere.emma.api

import java.io.{OutputStream, OutputStreamWriter}

import au.com.bytecode.opencsv.CSVWriter

abstract class OutputFormat[T] {
  def write(in: DataBag[T], os: OutputStream): Unit
}

class CSVOutputFormat[T: CSVConverters](val separator: Char) extends OutputFormat[T] {

  val convert = implicitly[CSVConverters[T]]

  def this() = this('\t')

  override def write(in: DataBag[T], os: OutputStream): Unit = {
    val writer = new CSVWriter(new OutputStreamWriter(os), separator, CSVWriter.NO_QUOTE_CHARACTER)

    in.fold()(x => writer.writeNext(convert.toCSV(x, separator)), (_, _) => ())

    writer.close()
  }
}