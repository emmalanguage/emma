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

class KFoldFlinkSpec extends KFoldSpec with FlinkAware {

  override protected def split(pdf: Seq[Double], seed: Long, xs: Seq[LDPoint[Int, Int]]) =
    withDefaultFlinkEnv(implicit flink => emma.onFlink {
      val folds = kfold.split(pdf)(DataBag(xs))(seed)
      folds.collect()
    })

  override protected def splitAndCount(pdf: Seq[Double], seed: Long, xs: Seq[LDPoint[Int, Int]]) =
    withDefaultFlinkEnv(implicit flink => emma.onFlink {
      val folds = kfold.split(pdf)(DataBag(xs))(seed)
      val sizes = for (g <- folds.groupBy(_.foldID)) yield g.key -> g.values.size
      sizes.collect().toMap
    })

  override protected def splitAndProject(pdf: Seq[Double], seed: Long, xs: Seq[LDPoint[Int, Int]]) =
    withDefaultFlinkEnv(implicit flink => emma.onFlink {
      val folds = kfold.split(pdf)(DataBag(xs))(seed)
      for (k <- pdf.indices) yield {
        val us = kfold.select(k)(folds).collect()
        val vs = kfold.except(k)(folds).collect()
        (us, vs)
      }
    })
}
