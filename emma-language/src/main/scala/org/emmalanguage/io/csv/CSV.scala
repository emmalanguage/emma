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

import io.Format

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

import CSV._

/**
 * CSV format configuration.
 * @param header     Indicate the presence of a CSV header (to be ignored when reading).
 * @param delimiter  Column delimiter character.
 * @param delimitSeq Sequence delimiter character (used by CSV converters for collections).
 * @param charset    Character set.
 * @param quote      Delimiters inside quotes are ignored.
 * @param escape     Escaped characters are always treated as data.
 * @param comment    Lines starting with a comment character are ignored.
 * @param nullValue  Fields matching this string will be set to null.
 */
case class CSV(
    //@formatter:off
    header     : Boolean      = defaultHeader,
    delimiter  : Char         = defaultDelimiter,
    delimitSeq : Char         = defaultDelimitSeq,
    charset    : Charset      = defaultCharset,
    quote      : Option[Char] = defaultQuote,
    escape     : Option[Char] = defaultEscape,
    comment    : Option[Char] = defaultComment,
    nullValue  : String       = defaultNullValue
    //@formatter:on
) extends Format {
  require({
    val chars = Seq(Some(delimiter), Some(delimitSeq), quote, escape, comment).flatten
    chars.distinct.size == chars.size
  }, "Please choose unique punctuation characters")
}

object CSV {
  //@formatter:off
  val defaultHeader       = false
  val defaultDelimiter    = '\t'
  val defaultDelimitSeq   = ';'
  val defaultCharset      = StandardCharsets.UTF_8
  val defaultQuote        = Some('"')
  val defaultEscape       = Some('\\')
  val defaultComment      = None
  val defaultNullValue    = ""
  //@formatter:on
  val default = CSV()
}
