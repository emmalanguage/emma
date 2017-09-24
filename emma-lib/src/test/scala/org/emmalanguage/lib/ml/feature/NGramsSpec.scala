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
package lib.ml.feature

import api._
import lib.ml.FPoint

class NGramsSpec extends FeatureSpec {

  val `2-grams` = tokenss.map(_.sliding(2).map(_.mkString(" ")).toArray)

  val `3-grams` = tokenss.map(_.sliding(3).map(_.mkString(" ")).toArray)

  "nGrams" should "compute correct 2-grams" in {
    val exp = for {
      (f, i) <- `2-grams`.zipWithIndex
    } yield FPoint(i, f.toSeq)
    val act = run(2)(tokenss.zipWithIndex)
    act.toList shouldEqual exp.toList
  }

  it should "compute correct 3-grams" in {
    val exp = for {
      (f, i) <- `3-grams`.zipWithIndex
    } yield FPoint(i, f.toSeq)
    val act = run(3)(tokenss.zipWithIndex)
    act.toList shouldEqual exp.toList
  }

  protected def run(n: Int)(xs: Seq[(Array[String], Int)]) = {
    val rs = for {
      (tokens, id) <- DataBag(xs)
    } yield FPoint(id, nGrams(n)(tokens).toSeq)
    rs.collect()
  }
}
