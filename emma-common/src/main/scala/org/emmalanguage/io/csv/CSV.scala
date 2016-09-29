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

import java.nio.charset.{Charset, StandardCharsets}

import CSV._

/** CSV format. */
case class CSV
(
  //@formatter:off
  header    : Boolean      = DEFAULT_HEADER,     // Indicate the presence of a CSV header (to be ignored)
  delimiter : Char         = DEFAULT_DELIMITER,  // Column delimiter character
  charset   : Charset      = DEFAULT_CHARSET,    // Character set
  quote     : Option[Char] = DEFAULT_QUOTE,      // Delimiters inside quotes are ignored
  escape    : Option[Char] = DEFAULT_ESCAPE,     // Escaped quote characters are ignored
  comment   : Option[Char] = DEFAULT_COMMENT,    // Disable comments by setting this to None
  nullValue : String       = DEFAULT_NULLVALUE   // Fields matching this string will be set as nulls
  //@formatter:on
) extends Format

object CSV {
  //@formatter:off
  val DEFAULT_HEADER       = false
  val DEFAULT_DELIMITER    = '\t'
  val DEFAULT_CHARSET      = StandardCharsets.UTF_8
  val DEFAULT_QUOTE        = Some('"')
  val DEFAULT_ESCAPE       = Some('\\')
  val DEFAULT_COMMENT      = None
  val DEFAULT_NULLVALUE    = ""
  //@formatter:on
}