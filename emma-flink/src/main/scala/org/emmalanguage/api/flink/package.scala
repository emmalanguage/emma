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

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.{ExecutionEnvironment => FlinkEnv}

package object flink {

  import FlinkDataSet.typeInfoForType
  import Meta.Projections.ctagFor

  implicit def fromDataSet[A](
    implicit flink: FlinkEnv, m: Meta[A]
  ): DataSet[A] => DataBag[A] = new FlinkDataSet(_)

  implicit def toDataSet[A](
    implicit flink: FlinkEnv, m: Meta[A]
  ): DataBag[A] => DataSet[A] = {
    case bag: FlinkDataSet[A] =>
      bag.rep
    case bag: ScalaSeq[A] =>
      flink.fromCollection(bag.rep)
    case bag =>
      throw new RuntimeException(s"Cannot convert a DataBag of type ${bag.getClass.getSimpleName} to a Flink DataSet")
  }

}
