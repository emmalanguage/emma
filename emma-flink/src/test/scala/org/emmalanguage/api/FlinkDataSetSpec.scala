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
package api

import io.csv.CSV
import io.csv.CSVConverter
import test.util.addToClasspath
import test.util.deleteRecursive
import test.util.tempPath

import org.apache.flink.api.scala.{ExecutionEnvironment => FlinkEnv}
import org.scalatest.BeforeAndAfter

import java.io.File

class FlinkDataSetSpec extends DataBagSpec with BeforeAndAfter {

  override type Bag[A] = FlinkDataSet[A]
  override type BackendContext = FlinkEnv

  override val Bag = FlinkDataSet
  override val suffix = "flink"

  override def withBackendContext[T](f: BackendContext => T): T =
    f(FlinkEnv.getExecutionEnvironment)

  override def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(implicit flink: FlinkEnv): DataBag[A] =
    FlinkDataSet.readCSV(path, format)

  val codegenDir = new File(tempPath("codegen"))

  before {
    // make sure that the base paths exist
    codegenDir.mkdirs()
    addToClasspath(codegenDir)
  }

  after {
    deleteRecursive(codegenDir)
  }
}
