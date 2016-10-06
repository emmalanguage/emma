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

import org.emmalanguage.io.csv.{CSV, CSVConverter}

import org.apache.flink.api.scala.{ExecutionEnvironment => FlinkEnv}

class FlinkDataSetSpec extends DataBagSpec {

  override type Bag[A] = FlinkDataSet[A]
  override type BackendContext = FlinkEnv

  override val suffix = "flink"

  override def withBackendContext[T](f: BackendContext => T): T =
    f(FlinkEnv.getExecutionEnvironment)

  override def Bag[A: Meta](implicit flink: FlinkEnv): Bag[A] =
    FlinkDataSet[A]

  override def Bag[A: Meta](seq: Seq[A])(implicit flink: FlinkEnv): Bag[A] =
    FlinkDataSet(seq)

  override def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(implicit flink: FlinkEnv): DataBag[A] =
    FlinkDataSet.readCSV(path, format)
}
