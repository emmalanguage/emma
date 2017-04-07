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
package io.text

import io.EmptyFormat
import io.ScalaSupport

import resource._

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets

/** A [[ScalaSupport]] implementation for reading and writing [[String]]. */
object TextSupport extends ScalaSupport[String, EmptyFormat.type] {

  override val format = EmptyFormat

  private[emmalanguage] def read(path: String): TraversableOnce[String] =
    new Traversable[String] {
      def foreach[U](f: String => U) = for {
        inp <- managed(inpStream(new URI(path)))
        isr <- managed(new InputStreamReader(inp, StandardCharsets.UTF_8))
        brd <- managed(new BufferedReader(isr))
      } {
        var record = brd.readLine()
        while (record != null) {
          f(record)
          record = brd.readLine()
        }
      }
    }

  private[emmalanguage] def write(path: String)(xs: Traversable[String]): Unit =
    for {
      out <- managed(outStream(new URI(path)))
      osw <- managed(new OutputStreamWriter(out, StandardCharsets.UTF_8))
      bwr <- managed(new BufferedWriter(osw))
    } for (x <- xs) {
      bwr.write(x)
      bwr.write('\n')
    }
}
