#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package algorithms.ml.clustering

import algorithms.ml.model._

import breeze.linalg.{Vector => Vec}
import breeze.linalg._
import breeze.numerics.pow
import org.emmalanguage.api.Meta.Projections._
import org.emmalanguage.api._

@emma.lib
object KMeans {

  def apply[PID: Meta](
    k: Int, epsilon: Double, iterations: Int // hyper-parameters
  )(
    points: DataBag[Point[PID]] // data-parameters
  ): DataBag[Solution[PID]] = {

    val dimensions = points.map(_.vector.length).distinct.fetch()
    assert(dimensions.size == 1,
      "Multiple dimensions in input data. All vectors should have the same length.")
    val N = dimensions.head

    var bestSolution = DataBag.empty[Solution[PID]]
    var minSqrDist = 0.0
    val zeroVec = Vec.zeros[Double](N)

    for (i <- 1 to iterations) {
      // initialize forgy cluster means
      var change = 0.0
      var centroids = DataBag(points.sample(k))

      // initialize solution
      var solution = for (p <- points) yield {
        val closest = centroids.min(Ordering.fromLessThan { (m1, m2) =>
          val diff1 = p.vector - m1.vector
          val diff2 = p.vector - m2.vector
          (diff1 dot diff1) < (diff2 dot diff2)
        })

        val diff = p.vector - closest.vector
        Solution(p, closest.id, diff dot diff)
      }

      do {
        // update means
        val newMeans = for (Group(clusterID, associatedPoints) <- solution.groupBy {
          _.clusterID
        }) yield {
          val sum = associatedPoints.fold(zeroVec)(_.point.vector, _ + _)
          val cnt = associatedPoints.size.toDouble
          Point(clusterID, sum / cnt)
        }

        // compute change between the old and the new means
        change = (for {
          mean <- centroids
          newMean <- newMeans
          if mean.id == newMean.id
        } yield sum(pow(mean.vector - newMean.vector, 2))).sum

        // update solution: re-assign clusters
        solution = for (s <- solution) yield {
          val closest = centroids.min(Ordering.fromLessThan { (m1, m2) =>
            val diff1 = s.point.vector - m1.vector
            val diff2 = s.point.vector - m2.vector
            (diff1.t * diff1) < (diff2.t * diff2)
          })

          val diff = s.point.vector - closest.vector
          s.copy(clusterID = closest.id, sqrDist = diff dot diff)
        }

        // use new means for the next iteration
        centroids = newMeans
      } while (change > epsilon)

      val sumSqrDist = solution.map(_.sqrDist).sum
      if (i <= 1 || sumSqrDist < minSqrDist) {
        minSqrDist = sumSqrDist
        bestSolution = solution
      }
    }

    bestSolution
  }

  case class Solution[PID](point: Point[PID], clusterID: PID, sqrDist: Double = 0)

}
