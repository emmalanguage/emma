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

import Parquet._
import io.Format

import org.apache.hadoop.conf.Configuration

/**
 * Parquet format.
 * @param binaryAsString   Interpret binary data as a string.
 * @param int96AsTimestamp Interpret INT96 data as a timestamp.
 * @param cacheMetadata    Turns on caching of Parquet schema metadata.
 * @param validation       Should validation be enabled?
 * @param dictEncoding     Should dictionary encoding be enabled?
 * @param codec            Compression codec use when writing Parquet files.
 */
case class Parquet(
    //@formatter:off
    binaryAsString   : Boolean       = defaultBinaryAsString,
    int96AsTimestamp : Boolean       = defaultInt96AsTimestamp,
    cacheMetadata    : Boolean       = defaultCacheMetadata,
    validation       : Boolean       = defaultValidation,
    dictEncoding     : Boolean       = defaultDictEncoding,
    codec            : Codec.Value   = Codec.Snappy,
    blockSize        : Option[Int]   = None,
    pageSize         : Option[Int]   = None,
    dictPageSize     : Option[Int]   = None,
    memPoolRatio     : Option[Float] = None,
    minMemAlloc      : Option[Long]  = None,
    maxPadding       : Option[Int]   = None
    //@formatter:on
  ) extends Format {

  val config = new Configuration()
  config.setBoolean("parquet.enable.summary-metadata", cacheMetadata)
  config.setBoolean("parquet.validation", validation)
  config.setBoolean("parquet.enable.dictionary", dictEncoding)
  config.set("parquet.compression", codec.toString.toUpperCase)
  for (block <- blockSize)    config.setInt("parquet.block.size", block)
  for (page  <- pageSize)     config.setInt("parquet.page.size", page)
  for (dict  <- dictPageSize) config.setInt("parquet.dictionary.page.size", dict)
  for (ratio <- memPoolRatio) config.setFloat("parquet.memory.pool.ratio", ratio)
  for (alloc <- minMemAlloc)  config.setLong("parquet.memory.min.chunk.size", alloc)
  for (pad   <- maxPadding)   config.setInt("parquet.writer.max-padding", pad)
}

object Parquet {
  //@formatter:off
  val defaultBinaryAsString    = false
  val defaultInt96AsTimestamp  = true
  val defaultCacheMetadata     = true
  val defaultValidation        = true
  val defaultDictEncoding      = true
  //@formatter:on
  val default = Parquet()

  object Codec extends Enumeration {
    val Uncompressed = Value("uncompressed")
    val Snappy = Value("snappy")
    val GZip = Value("gzip")
    val LZO = Value("lzo")
  }
}
