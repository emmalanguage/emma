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
package io.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api._
import org.apache.parquet.schema._

import scala.Function.const
import scala.collection.JavaConverters.mapAsJavaMapConverter

private[parquet] class ParquetWriteSupport[A](implicit converter: ParquetConverter[A]) extends WriteSupport[A] {
  var consumer: RecordConsumer = _
  var callback: A => Unit = const(())

  val schema = {
    val schema = converter.schema
    if (schema.isPrimitive) new MessageType(ParquetConverter.defaultName, schema)
    else new MessageType(schema.getName, schema.asGroupType.getFields)
  }

  def init(configuration: Configuration) = {
    val metadata = Map.empty[String, String].asJava
    new WriteSupport.WriteContext(schema, metadata)
  }

  def prepareForWrite(recordConsumer: RecordConsumer) = {
    consumer = recordConsumer
    callback = converter.writer(consumer, top = true)
  }

  def write(record: A) =
    callback(record)
}

private[parquet] object ParquetWriteSupport {
  def builder[A: ParquetConverter](file: Path): WriterBuilder[A] =
    new WriterBuilder[A](new ParquetWriteSupport, file)

  class WriterBuilder[A](support: WriteSupport[A], file: Path)
    extends ParquetWriter.Builder[A, WriterBuilder[A]](file) {

    def getWriteSupport(conf: Configuration) = support

    def self() = this
  }

}
