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

import api.Meta.Projections._
import api._
import lib.linalg._
import lib.ml._
import lib.stats._

@emma.lib
object kMeans {

  def apply[PID: Meta](
    D: Int, k: Int, epsilon: Double, iterations: Int // hyper-parameters
  )(
    points: DataBag[DPoint[PID]] // data-parameters
  ): DataBag[Solution[PID]] = {
    // helper method: orders points `x` based on their distance to `pos`
    val distanceTo = (pos: DVector) => Ordering.by { x: DPoint[PID] =>
      sqdist(pos, x.pos)
    }

    var optSolution = DataBag.empty[Solution[PID]]
    var minSqrDist = 0.0

    for (i <- 1 to iterations) {
      // initialize forgy cluster means
      var delta = 0.0
      var ctrds = DataBag(points.sample(k))

      // initialize solution: label points with themselves
      var solution = for (p <- points) yield LDPoint(p.id, p.pos, p)

      do {
        // update solution: label each point with its nearest cluster
        solution = for (s <- solution) yield {
          val closest = ctrds.min(distanceTo(s.pos))
          s.copy(label = closest)
        }
        // update centroid positions as mean of associated points
        val newCtrds = for {
          Group(cid, ps) <- solution.groupBy(_.label.id)
        } yield {
          val sum = stat.sum(D)(ps.map(_.pos))
          val cnt = ps.size.toDouble
          val avg = sum * (1 / cnt)
          DPoint(cid, avg)
        }

        // update delta as the sum of squared distances between the old and the new means
        delta = (for {
          cOld <- ctrds
          cNew <- newCtrds
          if cOld.id == cNew.id
        } yield sqdist(cOld.pos, cNew.pos)).sum

        // use new means for the next iteration
        ctrds = newCtrds
      } while (delta > epsilon)

      val sumSqrDist = (for (p <- solution) yield {
        sqdist(p.label.pos, p.pos)
      }).sum

      if (i <= 1 || sumSqrDist < minSqrDist) {
        minSqrDist = sumSqrDist
        optSolution = solution
      }
    }

    optSolution
  }

  type Solution[PID] = LDPoint[PID, DPoint[PID]]

}
