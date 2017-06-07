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
import test.util._

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import java.io.File

trait BaseWordCountIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val codegenDir = tempPath("codegen")
  val dir = "/text"
  val path = tempPath(dir)

  before {
    new File(codegenDir).mkdirs()
    new File(path).mkdirs()
    addToClasspath(new File(codegenDir))
    materializeResource(s"$dir/jabberwocky.txt")
  }

  after {
    deleteRecursive(new File(codegenDir))
    deleteRecursive(new File(path))
  }

  it should "count words" in {
    wordCount(s"$path/jabberwocky.txt", s"$path/wordcount-output.txt", CSV())

    val act = DataBag(fromPath(s"$path/wordcount-output.txt"))
    val exp = DataBag({
      val words = for {
        line <- fromPath(s"$path/jabberwocky.txt")
        word <- line.toLowerCase.split("\\W+")
        if word != ""
      } yield word

      for {
        (word, occs) <- words.groupBy(x => x).toSeq
      } yield s"$word\t${occs.length}"
    })

    act.collect() should contain theSameElementsAs exp.collect()
  }

  def wordCount(input: String, output: String, csv: CSV): Unit
}
