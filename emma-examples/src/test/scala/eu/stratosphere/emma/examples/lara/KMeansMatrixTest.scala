package eu.stratosphere.emma.examples.lara

import eu.stratosphere.emma.api.lara.{BaseTest, Matrix, Vector}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class KMeansMatrixTest extends BaseTest{

  //parameters
  val variance = 8
  val maxIterations = 100
  val numPoints = 1000
  val epsilon = 0.000001


  "Trained centroids" - {

    "2 clusters" in {
      //generate data with 2 clusters
      val x2 = Matrix.fill(numPoints, 2)((i, j) =>
        if (i < numPoints / 2) 0 + variance * Random.nextDouble()
        else 10 + variance * Random.nextDouble()
      )

      //compute the true centroids for x2 and k = 2
      val c1Points = Matrix.fillRows(numPoints / 2, 2)(i => x2.row(i))
      val c2Points = Matrix.fillRows(numPoints / 2 + numPoints % 2, 2)(i => x2.row(numPoints / 2 + i))
      val c1 = Vector.fill(2)(j => c1Points.column(j).aggregate(_ + _)) / (numPoints / 2)
      val c2 = Vector.fill(2)(j => c2Points.column(j).aggregate(_ + _)) / (numPoints / 2 + numPoints % 2)
      val trueCentroidsX2 = Set(c1.transpose(), c2.transpose())

      //compute the trained centroids
      val centroids2 = KMeansMatrix.kMeans(x2, 2, maxIterations, epsilon)

      centroids2.numRows shouldBe 2
      centroids2.numCols shouldBe 2
      Set(centroids2.row(0), centroids2.row(1)) shouldBe trueCentroidsX2
    }

    "3 clusters" in {
      //generate data with 3 clusters
      val x3 = Matrix.fill(numPoints,2)((i,j) =>
        if (i < numPoints / 3) {0 + variance * Random.nextDouble()}
        else if (i < 2 * (numPoints / 3)) {10 + variance * Random.nextDouble()}
        else {20 + variance * Random.nextDouble()}
      )
      //compute the true centroids for x3 and k = 3
      val c1x3Points = Matrix.fillRows(numPoints/3, 2)(i => x3.row(i))
      val c2x3Points = Matrix.fillRows(numPoints/3, 2)(i => x3.row(numPoints/3 + i))
      val c3x3Points = Matrix.fillRows(numPoints/3 + numPoints % 3,2)(i => x3.row(2 * (numPoints/3) + i))
      val c1x3 = Vector.fill(2)(j => c1x3Points.column(j).aggregate(_ + _)) / (numPoints/3)
      val c2x3 = Vector.fill(2)(j => c2x3Points.column(j).aggregate(_ + _)) / (numPoints/3)
      val c3x3 = Vector.fill(2)(j => c3x3Points.column(j).aggregate(_ + _)) / (numPoints/3 + numPoints%3)
      val trueCentroidsX3 = Set(c1x3.transpose(),c2x3.transpose(),c3x3.transpose())

      //compute the trained centroids
      val centroids3 = KMeansMatrix.kMeans(x3,3,maxIterations,epsilon)

      //check adequation
      centroids3.numRows shouldBe 3
      centroids3.numCols shouldBe 2
      Set(centroids3.row(0),centroids3.row(1),centroids3.row(2)) shouldBe trueCentroidsX3
    }

    "with empty clusters" in {
      //generate matrix representing 10 points
      val x = Matrix.fill(10,2)((i,j) => (i + j).toDouble)
      //with k >= 10, the true centroids should be the points themselves
      var trueCentroidsSet: Set[Vector[Double]] = Set.empty
      for (i <- 0 until 10) trueCentroidsSet = trueCentroidsSet + x.row(i)

      //compute trained centroids with k = 11 so one cluster (at least) will always be empty
      val trainedCentroids = KMeansMatrix.kMeans(x,11,maxIterations,epsilon)
      var trainedCentroidsSet: Set[Vector[Double]] = Set.empty
      for (i <- 0 until 11) trainedCentroidsSet = trainedCentroidsSet + trainedCentroids.row(i)

      //trained centroids should contain the 10 points + some duplicates so trainedSet and trueSet should be equal
      trainedCentroids.numRows shouldBe 11
      trainedCentroids.numCols shouldBe 2
      trainedCentroidsSet.size shouldBe 10
      trainedCentroidsSet shouldBe trueCentroidsSet
    }
  }
}