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

import io.ScalaSupport

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileWriter

class ParquetScalaSupport[A: ParquetConverter](override val format: Parquet)
  extends ScalaSupport[A, Parquet] {

  private[emmalanguage] def read(path: String): TraversableOnce[A] = {
    val builder = ParquetReadSupport
      .builder(new Path(path)).withConf(format.config)

    new Traversable[A] {
      def foreach[U](f: A => U) = {
        var exception: Throwable = null
        val parquet = builder.build()
        try Iterator.continually(parquet.read)
          .takeWhile(_ != null).foreach(f)
        catch { case ex: Throwable =>
          exception = ex
          throw ex
        } finally if (parquet != null) {
          if (exception != null) try {
            parquet.close()
          } catch { case ex: Throwable =>
            exception.addSuppressed(ex)
          } else parquet.close()
        }
      }
    }
  }

  private[emmalanguage] def write(path: String)(xs: Traversable[A]): Unit = {
    var exception: Throwable = null
    val parquet = ParquetWriteSupport
      .builder(new Path(path)).withConf(format.config)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .build()

    try xs.foreach(parquet.write)
    catch { case ex: Throwable =>
      exception = ex
      throw ex
    } finally if (parquet != null) {
      if (exception != null) try {
        parquet.close()
      } catch { case ex: Throwable =>
        exception.addSuppressed(ex)
      } else parquet.close()
    }
  }
}

/** Companion object. */
object ParquetScalaSupport {

  def apply[A: ParquetConverter](format: Parquet): ParquetScalaSupport[A] =
    new ParquetScalaSupport[A](format)
}
