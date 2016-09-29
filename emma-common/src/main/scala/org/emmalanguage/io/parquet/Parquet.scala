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

import io.Format

import java.nio.charset.{Charset, StandardCharsets}

/** CSV format. */
case class Parquet
(
  //@formatter:off
  header    : Boolean      = false,                  // Indicate the presence of a CSV header (to be ignored)
  delimiter : Char         = '\t',                   // Column delimiter character
  charset   : Charset      = StandardCharsets.UTF_8, // Character set
  quote     : Option[Char] = Some('"'),              // Delimiters inside quotes are ignored
  escape    : Option[Char] = Some('\\'),             // Escaped quote characters are ignored
  comment   : Option[Char] = Some('#'),              // Disable comments by setting this to None
  nullValue : String       = ""                      // Fields matching this string will be set as nulls
  //@formatter:on
) extends Format