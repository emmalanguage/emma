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
import io.csv.CSV
import eu.stratosphere.emma.testutil.ExampleTest

import org.apache.spark.sql.SparkSession
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

// TODO: migrate this to `emma-spark` once the dependency to `emma-examples` is inverted
@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class SparkWordCountSpec extends BaseWordCountSpec {

  override def wordCount(input: String, output: String, csv: CSV): Unit =
    emma.onSpark {
      // read the input
      val docs = DataBag.readCSV[String](input, csv)
      // parse and count the words
      val counts = WordCount(docs)
      // write the results into a file
      counts.writeCSV(output, csv)
    }

  implicit lazy val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName(this.getClass.getSimpleName)
    .getOrCreate()
}
