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
package examples

//import api._
import test.util._

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import java.io.File

trait BaseClickCountDiffsIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val resourceDir = "/ClickCountDiffs"
  val codegenDir = tempPath("codegen")
  val tmpDir = tempPath(resourceDir)

  before {
    new File(codegenDir).mkdirs()
    new File(tmpDir).mkdirs()
    addToClasspath(new File(codegenDir))
    materializeResource(s"$resourceDir/clickLog_1")
    materializeResource(s"$resourceDir/clickLog_2")
    materializeResource(s"$resourceDir/clickLog_3")
    materializeResource(s"$resourceDir/clickLog_4")
  }

  after {
    deleteRecursive(new File(codegenDir))
    deleteRecursive(new File(tmpDir))
  }

  it should "xxxx" in {
    clickCountDiffs(s"$tmpDir/clickLog_", 4)

    //todo: check output
  }

  def clickCountDiffs(baseInName: String, numDays: Int): Unit
}
