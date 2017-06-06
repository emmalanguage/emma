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

import io.ScalaSupport

import com.univocity.parsers.csv.CsvParser
import com.univocity.parsers.csv.CsvParserSettings
import com.univocity.parsers.csv.CsvWriter
import com.univocity.parsers.csv.CsvWriterSettings
import resource._

import java.io._
import java.net.URI

/** A [[ScalaSupport]] implementation for the [[CSV]] [[io.Format]]. */
class CSVScalaSupport[A](val format: CSV)(implicit conv: CSVConverter[A])
  extends ScalaSupport[A, CSV] {

  private[emmalanguage] def read(path: String): TraversableOnce[A] =
    new Traversable[A] {
      def foreach[U](f: A => U) = for {
        inp <- managed(inpStream(new URI(path)))
        bis <- managed(new BufferedInputStream(inp))
        isr <- managed(new InputStreamReader(bis, format.charset))
      } {
        val csv = new CsvParser(parserSettings(format))
        csv.beginParsing(isr)
        var record = csv.parseNext()
        while (record != null) {
          f(conv.read(record, 0)(format))
          record = csv.parseNext()
        }
        csv.stopParsing()
      }
    }

  private[emmalanguage] def write(path: String)(xs: Traversable[A]): Unit = {
    val record = Array.ofDim[String](conv.size)
    for {
      out <- managed(outStream(new URI(path)))
      bos <- managed(new BufferedOutputStream(out))
      osw <- managed(new OutputStreamWriter(bos, format.charset))
      csv <- managed(new CsvWriter(osw, writerSettings(format)))
    } for (x <- xs) {
      conv.write(x, record, 0)(format)
      csv.writeRow(record)
    }
  }

  private[emmalanguage] def parser(): CsvParser =
    new CsvParser(parserSettings(format))

  private[emmalanguage] def writer(): CsvWriter =
    new CsvWriter(writerSettings(format))

  // ---------------------------------------------------------------------------
  // Helper functions for reading
  // ---------------------------------------------------------------------------

  private def writerSettings(format: CSV): CsvWriterSettings = {
    val settings = new CsvWriterSettings()

    // derived from the CSV format
    settings.getFormat.setDelimiter(format.delimiter)
    format.quote.foreach(quote => settings.getFormat.setQuote(quote))
    format.escape.foreach(escape => settings.getFormat.setQuoteEscape(escape))
    format.comment.foreach(comment => settings.getFormat.setComment(comment))
    settings.setNullValue(format.nullValue)
    // hard-coded
    settings.setIgnoreLeadingWhitespaces(true)
    settings.setIgnoreTrailingWhitespaces(true)

    settings
  }

  private def parserSettings(format: CSV): CsvParserSettings = {
    val settings = new CsvParserSettings()

    // derived from the CSV format
    settings.setHeaderExtractionEnabled(format.header)
    settings.getFormat.setDelimiter(format.delimiter)
    format.quote.foreach(quote => settings.getFormat.setQuote(quote))
    format.escape.foreach(escape => settings.getFormat.setQuoteEscape(escape))
    format.comment.foreach(comment => settings.getFormat.setComment(comment))
    settings.setNullValue(format.nullValue)
    settings.setNumberOfRowsToSkip(format.skipRows)
    // hard-coded
    settings.setIgnoreLeadingWhitespaces(true)
    settings.setIgnoreTrailingWhitespaces(true)

    settings
  }
}

/** Companion object. */
object CSVScalaSupport {

  def apply[A: CSVConverter](format: CSV): CSVScalaSupport[A] =
    new CSVScalaSupport[A](format)
}
