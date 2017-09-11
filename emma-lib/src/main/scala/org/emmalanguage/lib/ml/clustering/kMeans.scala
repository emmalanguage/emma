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
package lib.ml.clustering

import api._
import lib.linalg._
import lib.ml._
import lib.stats._
import util.RanHash

@emma.lib
object kMeans {

  /**
   * K-Means clustering algorithm.
   *
   * @param D          Number of dimensions
   * @param k          Number of clusters
   * @param runs       Number of runs.
   * @param iterations Number of iterations.
   * @param distance   Distance metric.
   * @param seed       Centroids seed.
   * @param points     Input points.
   * @tparam PID Point identity type.
   */
  def apply[PID: Meta](
    D: Int,
    k: Int,
    runs: Int,
    iterations: Int,
    distance: (DVector, DVector) => Double = sqdist,
    seed: Long = 452642543145L
  )(
    points: DataBag[DPoint[PID]]
  ): DataBag[Solution[PID]] = {
    // helper method: orders points `x` based on their distance to `pos`
    val distanceTo = (pos: DVector) => Ordering.by((x: DPoint[PID]) => distance(pos, x.pos))

    var optSolution = DataBag.empty[Solution[PID]]
    var minDistance = Double.MaxValue

    for (run <- 1 to runs) {
      // initialize forgy cluster means
      var centroids = DataBag(points.sample(k, RanHash(seed, run).seed))

      for (_ <- 0 until iterations) {
        // update solution: label each point with its nearest cluster
        val solution = for (p <- points) yield {
          val closest = centroids.min(distanceTo(p.pos))
          LDPoint(p.id, p.pos, closest)
        }
        // update centroid positions as mean of associated points
        centroids = for {
          Group(cid, ps) <- solution.groupBy(_.label.id)
        } yield {
          val sum = stat.sum(D)(ps.map(_.pos))
          val cnt = ps.size.toDouble
          val avg = sum * (1 / cnt)
          DPoint(cid, avg)
        }
      }

      val solution = for (p <- points) yield {
        val closest = centroids.min(distanceTo(p.pos))
        LDPoint(p.id, p.pos, closest)
      }

      val sumDistance = (for (p <- solution) yield {
        distance(p.label.pos, p.pos)
      }).sum

      if (run <= 1 || sumDistance < minDistance) {
        minDistance = sumDistance
        optSolution = solution
      }
    }

    optSolution
  }

  type Solution[PID] = LDPoint[PID, DPoint[PID]]

}
