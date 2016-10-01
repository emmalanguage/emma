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
package eu.stratosphere.emma.examples.imdb

import eu.stratosphere.emma.testutil._

import java.io.File

import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class IMDbTest extends FunSuite with Matchers with BeforeAndAfter {
  // default parameters
  val dir = "/cinema"
  val path = tempPath(dir)

  before {
    new File(s"$path/output").mkdirs()
    materializeResource(s"$dir/imdb.csv")
    materializeResource(s"$dir/berlinalewinners.csv")
    materializeResource(s"$dir/canneswinners.csv")
  }

  after {
    deleteRecursive(new File(path))
  }

  test("IMDb") (withRuntime() { rt =>
    new IMDb(path, s"$path/output", rt).run()
  })
}
