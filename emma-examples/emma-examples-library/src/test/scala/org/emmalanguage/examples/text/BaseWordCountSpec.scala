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
package examples.text

import api._
import io.csv.CSV
import test.util._

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

trait BaseWordCountSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val dir = "/text/"
  val path = tempPath(dir)
  val text = "To be or not to Be"

  before {
    new File(path).mkdirs()
    Files.write(Paths.get(s"$path/hamlet.txt"), text.getBytes(StandardCharsets.UTF_8))
  }

  after {
    deleteRecursive(Paths.get(path).toFile)
  }

  "WordCount" should "count words" in {
    wordCount(s"$path/hamlet.txt", s"$path/output.txt", CSV())

    val act = DataBag(fromPath(s"$path/output.txt"))
    val exp = DataBag(text.toLowerCase.split("\\W+").groupBy(x => x).toSeq.map(x => s"${x._1}\t${x._2.length}"))

    compareBags(act.fetch(), exp.fetch())
  }

  def wordCount(input: String, output: String, csv: CSV): Unit
}
