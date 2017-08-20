#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.ml.clustering

import algorithms.ml.model._

import breeze.linalg._
import org.emmalanguage.api._

@emma.lib
object KMeans {

  def apply[PID: Meta](
    D: Int, k: Int, epsilon: Double, iterations: Int // hyper-parameters
  )(
    points: DataBag[Point[PID]] // data-parameters
  ): DataBag[Solution[PID]] = {
    // helper method: orders points `x` based on their distance to `pos`
    val distanceTo = (pos: Array[Double]) => Ordering.by { x: Point[PID] =>
      squaredDistance(Vector(pos), Vector(x.pos))
    }

    // helper fold algebra: sum positions of labeled (solution) points
    val Sum = alg.Fold[Solution[PID], Vector[Double]](
      z = Vector.zeros[Double](D),
      i = x => Vector(x.pos),
      p = _ + _
    )

    var optSolution = DataBag.empty[Solution[PID]]
    var minSqrDist = 0.0

    for (i <- 1 to iterations) {
      // initialize forgy cluster means
      var delta = 0.0
      var ctrds = DataBag(points.sample(k))

      // initialize solution: label points with themselves
      var solution = for (p <- points) yield LPoint(p.id, p.pos, p)

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
          val sum = ps.fold(Sum)
          val cnt = ps.size.toDouble
          Point(cid, (sum / cnt).toArray)
        }

        // update delta as the sum of squared distances between the old and the new means
        delta = (for {
          cOld <- ctrds
          cNew <- newCtrds
          if cOld.id == cNew.id
        } yield squaredDistance(Vector(cOld.pos), Vector(cNew.pos))).sum

        // use new means for the next iteration
        ctrds = newCtrds
      } while (delta > epsilon)

      val sumSqrDist = (for (p <- solution) yield {
        squaredDistance(Vector(p.label.pos), Vector(p.pos))
      }).sum

      if (i <= 1 || sumSqrDist < minSqrDist) {
        minSqrDist = sumSqrDist
        optSolution = solution
      }
    }

    optSolution
  }

  type Solution[PID] = LPoint[PID, Point[PID]]

}
