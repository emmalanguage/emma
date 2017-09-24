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
package lib.ml

import api._
import lib.BaseLibSpec
import lib.linalg._

class KFoldSpec extends BaseLibSpec {

  val N = 1000
  val e = 0.05

  val s1 = 54326427L
  val s2 = 23546473L
  val xs = (1 to N).map(i =>LDPoint(i, dense(Array(i.toDouble)), i))
  val fs = Seq(0.3, 0.2, 0.5)

  "kfold.split" should "return the same assignment with a matching pdf and seed" in {
    val rs1 = split(fs, s1, xs)
    val rs2 = split(fs, s1, xs)
    rs1 should contain theSameElementsAs rs2
  }

  it should "assign folds proportionally to PDF" in {
    val rm1 = splitAndCount(fs, s1, xs)
    val rm2 = splitAndCount(fs, s2, xs)
    for ((p, i) <- fs.zipWithIndex) {
      rm1(i) / N.toDouble shouldBe (p +- e)
      rm2(i) / N.toDouble shouldBe (p +- e)
    }
  }

  it should "compute non-overlapping folds" in {
    val rs1 = splitAndProject(fs, s1, xs)
    val rs2 = splitAndProject(fs, s1, xs)
    for ((us, vs) <- rs1) {
      us ++ vs should contain theSameElementsAs xs
      us intersect vs shouldBe empty
    }
    for ((us, vs) <- rs2) {
      us ++ vs should contain theSameElementsAs xs
      us intersect vs shouldBe empty
    }
  }

  protected def split(pdf: Seq[Double], seed: Long, xs: Seq[LDPoint[Int, Int]]) = {
    val folds = kfold.split(pdf)(DataBag(xs))(seed)
    folds.collect()
  }

  protected def splitAndCount(pdf: Seq[Double], seed: Long, xs: Seq[LDPoint[Int, Int]]) = {
    val folds = kfold.split(pdf)(DataBag(xs))(seed)
    val sizes = for (g <- folds.groupBy(_.foldID)) yield g.key -> g.values.size
    sizes.collect().toMap
  }

  protected def splitAndProject(pdf: Seq[Double], seed: Long, xs: Seq[LDPoint[Int, Int]]) = {
    val folds = kfold.split(pdf)(DataBag(xs))(seed)
    for (k <- pdf.indices) yield {
      val us = kfold.select(k)(folds).collect()
      val vs = kfold.except(k)(folds).collect()
      (us, vs)
    }
  }
}
