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

import test.util._

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import java.io.File

trait BaseClickCountDiffsIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfter {

  val resourceDir = "/ClickCountDiffs"
  val codegenDir = tempPath("codegen")
  val tmpDir = tempPath(resourceDir)

  val numDays = 4

  before {
    new File(codegenDir).mkdirs()
    new File(tmpDir).mkdirs()
    addToClasspath(new File(codegenDir))
    (1 to numDays).foreach { day =>
      materializeResource(s"$resourceDir/clickLog_" + day)
    }
    (2 to numDays).foreach { day =>
      materializeResource(s"$resourceDir/clickLog_" + day + ".exp")
    }
  }

  after {
    deleteRecursive(new File(codegenDir))
    deleteRecursive(new File(tmpDir))
  }

  it should "compare click counts" in {
    clickCountDiffs(s"$tmpDir/clickLog_", numDays)

    (2 to numDays).foreach { day =>
      scala.reflect.io.File(s"$tmpDir/clickLog_" + day + ".out").slurp().trim should equal (
        scala.reflect.io.File(s"$tmpDir/clickLog_" + day + ".exp").slurp().trim
      )
    }
  }

  def clickCountDiffs(baseName: String, numDays: Int): Unit
}
