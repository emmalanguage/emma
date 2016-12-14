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
package api.converter

import api.DataBag
import api.FlinkDataSet

import org.apache.flink.api.scala.DataSet

object flink {

  /** Converts a `DataBag[A]` into a Flink `DataSet[A]`. */
  implicit val datasetConverter = new CollConverter[DataSet] {
    override def apply[A](bag: DataBag[A]) = bag match {
      case bag: FlinkDataSet[A] =>
        bag.rep
      case _ =>
        throw new RuntimeException(s"Cannot convert a DataBag of type ${bag.getClass.getSimpleName} to Dataset")
    }
  }

}
