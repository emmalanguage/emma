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
import api.SparkDataset
import api.SparkRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import scala.language.higherKinds

object spark {

  /** Converts a `DataBag[A]` into a Spark `Dataset[A]`. */
  implicit val datasetConverter = new CollConverter[Dataset] {
    def apply[A](bag: DataBag[A]): Dataset[A] = bag match {
      case bag: SparkDataset[A] =>
        bag.rep
      case bag: SparkRDD[A] =>
        val encoder = SparkDataset.encoderForType[A](bag.m)
        bag.spark.createDataset(bag.rep)(encoder)
      case _ =>
        throw new RuntimeException(s"Cannot convert a DataBag of type ${bag.getClass.getSimpleName} to Dataset")
    }
  }

  /** Converts a `DataBag[A]` into a Spark `RDD[A]`. */
  implicit val rddConverter = new CollConverter[RDD] {
    def apply[A](bag: DataBag[A]): RDD[A] = bag match {
      case bag: SparkDataset[A] =>
        bag.rep.rdd
      case bag: SparkRDD[A] =>
        bag.rep
      case _ =>
        throw new RuntimeException(s"Cannot convert a DataBag of type ${bag.getClass.getSimpleName} to RDD")
    }
  }
}
