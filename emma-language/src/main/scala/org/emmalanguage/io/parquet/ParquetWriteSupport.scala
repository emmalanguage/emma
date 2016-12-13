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
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api._
import org.apache.parquet.schema._

import scala.Function.const
import scala.collection.JavaConverters.mapAsJavaMapConverter

private[parquet] class ParquetWriteSupport[A](implicit converter: ParquetConverter[A])
  extends WriteSupport[A] {
  import WriteSupport.WriteContext

  var consumer: RecordConsumer = _
  var callback: A => Unit = const(())
  val schema = new MessageType("actor", converter.schema)

  def init(configuration: Configuration): WriteContext = {
    val metadata = Map.empty[String, String].asJava
    new WriteSupport.WriteContext(schema, metadata)
  }

  def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    consumer = recordConsumer
    callback = converter.writer(consumer)
  }

  def write(record: A): Unit = {
    consumer.startMessage()
    consumer.startField(ParquetConverter.defaultField, 0)
    callback(record)
    consumer.endField(ParquetConverter.defaultField, 0)
    consumer.endMessage()
  }
}

private[parquet] object ParquetWriteSupport {
  def builder[A: ParquetConverter](file: Path): WriterBuilder[A] =
    new WriterBuilder[A](new ParquetWriteSupport, file)

  class WriterBuilder[A](support: WriteSupport[A], file: Path) {

    var conf = new Configuration()
    var mode = ParquetFileWriter.Mode.CREATE

    def getWriteSupport(conf: Configuration) = support
    def self() = this

    def withConf(conf: Configuration): WriterBuilder[A] = {
      this.conf = conf
      this
    }

    def withWriteMode(mode: ParquetFileWriter.Mode): WriterBuilder[A] = {
      this.mode = mode
      this
    }

    def build(): ParquetWriter[A] = new ParquetWriter[A](
      file,
      mode,
      support,
      ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
      ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
      ParquetWriter.DEFAULT_WRITER_VERSION,
      conf
    )
  }
}
