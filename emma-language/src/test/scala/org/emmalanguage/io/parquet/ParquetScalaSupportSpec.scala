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
package io.parquet

import test.util.tempPath

import org.scalatest.prop.PropertyChecks
import org.scalatest.FreeSpec
import org.scalatest.Matchers

class ParquetScalaSupportSpec extends FreeSpec with Matchers with PropertyChecks {

  case class Name(first: String, middle: Option[String], last: String)
  case class Movie(title: String, year: Int)
  case class Actor(name: Name, filmography: Seq[Movie])

  "basic example" in {
    val expected = Seq(
      Actor(Name("Harry", Some("Dean"), "Stanton"), Seq(
        Movie("Paris, Texas", 1984),
        Movie("Seven Psychopaths", 2012)
      )),
      Actor(Name("Jamie", Some("Lee"), "Courtis"), Seq(
        Movie("True Lies", 1994),
        Movie("A Fish Called Wanda", 1988)
      )),
      Actor(Name("Daniel", None, "Day-Lewis"), Seq(
        Movie("There Will Be Blood", 2007)
      ))
    )

    val parquet = new ParquetScalaSupport[Actor](Parquet.default)
    val path = tempPath("actors.parquet")
    parquet.write(path)(expected)
    val actual = parquet.read(path).toStream
    actual should contain theSameElementsInOrderAs expected
  }
}
