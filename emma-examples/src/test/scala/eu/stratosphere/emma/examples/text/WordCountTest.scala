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
package eu.stratosphere.emma.examples.text

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.testutil._

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

import java.io.File

import org.junit.experimental.categories.Category
import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class WordCountTest extends FlatSpec with Matchers with BeforeAndAfter {
  // default parameters
  val dir        = "/text/"
  val path       = tempPath(dir)
  val text       = "To be or not to Be"

  before {
    new File(path).mkdirs()
    Files.write(Paths.get(s"$path/hamlet.txt"), text.getBytes(StandardCharsets.UTF_8))
  }

  after {
    deleteRecursive(Paths.get(path).toFile)
  }

  "WordCount" should "count words" ignore withRuntime() { rt =>
    new WordCount(s"$path/hamlet.txt", s"$path/output.txt", rt).run()

    val act = DataBag(fromPath(s"$path/output.txt"))
    val exp = DataBag(text.toLowerCase.split("\\W+").groupBy(x => x).toSeq.map(x => s"${x._1}\t${x._2.length}"))

    compareBags(act.fetch(), exp.fetch())
  }
}
