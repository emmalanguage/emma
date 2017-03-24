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
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.api._
import org.apache.parquet.io.api._
import org.apache.parquet.schema._

import java.util

import scala.collection.JavaConverters.mapAsJavaMapConverter

private[parquet] class ParquetReadSupport[A](implicit converter: ParquetConverter[A]) extends ReadSupport[A] {
  import ReadSupport.ReadContext

  var current: A = _
  val reader = converter.reader(current = _, top = true).asGroupConverter

  val schema = {
    val schema = converter.schema
    if (schema.isPrimitive) new MessageType(ParquetConverter.defaultName, schema)
    else new MessageType(schema.getName, schema.asGroupType.getFields)
  }

  override def init(context: InitContext) = {
    val metadata = Map.empty[String, String].asJava
    new ReadContext(schema, metadata)
  }

  def prepareForRead(
    configuration: Configuration,
    keyValueMetaData: util.Map[String, String],
    fileSchema: MessageType,
    readContext: ReadContext
  ) = new RecordMaterializer[A] {
    def getRootConverter = reader
    def getCurrentRecord = current
  }
}

private[parquet] object ParquetReadSupport {
  def builder[A: ParquetConverter](file: Path): ParquetReader.Builder[A] =
    ParquetReader.builder(new ParquetReadSupport, file)
}
