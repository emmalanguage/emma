package eu.stratosphere.emma.examples.lara

import eu.stratosphere.emma.api.lara.{Matrix, Vector}

import scala.util.Random

/**
  * KMeans example implemented with the matrix abstraction of Emma
  */

object KMeansMatrix {

  /**
    * @param x matrix representing the points. Each row corresponds to a point
    * @param k number of clusters
    * @param maxIterations maximum steps before the algorithm stops if it has not converged
    * @param epsilon convergence criterion
    * @return centroids matrix with k rows where each row corresponds to a trained centroid
    */
  def kMeans(x: Matrix[Double], k: Int, maxIterations: Int, epsilon: Double): Matrix[Double] = {

    val m = x.numRows
    val n = x.numCols
    var change = 0.0
    var currentIteration = 0

    //initialisation of the centroids with k random points
    var centroids : Matrix[Double] = Matrix.fillColumns(n,k)(j => x.row(Random.nextInt(m)))
    var newCentroids : Matrix[Double] = centroids

    do {
      //computation of the matrix of the square distances
      val distances : Matrix[Double] = Matrix.fill(m,k)((i,j) =>
        (x.row(i) - newCentroids.column(j)).fold(0.0)(s => s * s, (l,r) => l + r)
      )

      //get the id of the nearest centroid for each point
      val minIdx = Vector.fill(m)( i =>
        distances.row(i).indexedFold((-1,Double.MaxValue))(
          s => (s.id,s.value),
          (l,r) => if (l._2 < r._2) l else r)._1
      )

      //computation of the matrix indicating where the points belong
      var z : Matrix[Double] = Matrix.fill(m,k)((i,j) => if (j == minIdx.get(i)) 1.0 else 0.0)
      //vector indicating the size of each cluster
      val clusterSizes = Vector.fill(k)(i => z.column(i).fold(0.0)(s => s, (l,r) => l + r))

      //build the list of alone centroids
      val emptyClusters = clusterSizes.indexedFold(List[Int]())(s =>
        if (s.value == 0) List(s.id) else List(),
        (l,r) => l:::r)

      //if there are empty clusters, the corresponding centroids are replaced by random points
      if (emptyClusters.nonEmpty) {
        z = Matrix.fillColumns(m,k)(j =>
          if (emptyClusters.contains(j)) {
            val c = Random.nextInt(m)
            Vector.fill(m)(v => if (v==c) 1.0 else 0.0)
          }
          else z.column(j)
        )
      }
      //compute new centroids
      centroids = newCentroids
      newCentroids = (x.transpose() %*% z).indexedCols(idx =>
        if (clusterSizes.get(idx.id) == 0.0) idx.value else idx.value / clusterSizes.get(idx.id)
      )
      currentIteration += 1

      //compute change
      change = Vector.fill(k)(i => (centroids.column(i) - newCentroids.column(i)).fold(0.0)(
        s => s * s, (l,r) => l + r)
        ).fold(0.0)( s => s, (l,r) => l + r)

    } while (change > epsilon && currentIteration < maxIterations )

    // return the trained model
    newCentroids.transpose()
  }
}
