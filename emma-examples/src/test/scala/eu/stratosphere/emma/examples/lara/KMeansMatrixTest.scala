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

  //generate data with 2 clusters
  val x2 = Matrix.fill(numPoints,2)((i,j) =>
    if (i < numPoints/2 ) 0 + variance * Random.nextDouble()
    else 10 + variance * Random.nextDouble()
  )

  //generate data with 3 clusters
  val x3 = Matrix.fill(numPoints,2)((i,j) =>
    if (i < numPoints / 3) {0 + variance * Random.nextDouble()}
    else if (i < 2 * (numPoints / 3)) {10 + variance * Random.nextDouble()}
    else {20 + variance * Random.nextDouble()}
  )

  //compute the true centroids for x2 and k = 2
  val c1Points = Matrix.fillRows(numPoints/2, 2)(i => x2.row(i))
  val c2Points = Matrix.fillRows(numPoints/2 + numPoints % 2,2)(i => x2.row(numPoints/2 + i))
  val c1 = Vector.fill(2)(j => c1Points.column(j).fold(0.0)(s => s, (l,r) => l + r)) / (numPoints / 2)
  val c2 = Vector.fill(2)(j => c2Points.column(j).fold(0.0)(s => s, (l,r) => l + r)) / (numPoints / 2 + numPoints % 2)
  val trueCentroidsX2 = List(c1.toArray.toList,c2.toArray.toList)

  //compute the true centroids for x3 and k = 3
  val c1x3Points = Matrix.fillRows(numPoints/3, 2)(i => x3.row(i))
  val c2x3Points = Matrix.fillRows(numPoints/3, 2)(i => x3.row(numPoints/3 + i))
  val c3x3Points = Matrix.fillRows(numPoints/3 + numPoints % 3,2)(i => x3.row(2 * (numPoints/3) + i))
  val c1x3 = Vector.fill(2)(j => c1x3Points.column(j).fold(0.0)(s=>s,(l,r) => l + r)) / (numPoints/3)
  val c2x3 = Vector.fill(2)(j => c2x3Points.column(j).fold(0.0)(s=>s,(l,r) => l + r)) / (numPoints/3)
  val c3x3 = Vector.fill(2)(j => c3x3Points.column(j).fold(0.0)(s=>s,(l,r) => l + r)) / (numPoints/3 + numPoints%3)
  val trueCentroidsX3 = List(c1x3.toArray.toList,c2x3.toArray.toList,c3x3.toArray.toList)

  //compute the trained centroids
  val centroids2 = KMeansMatrix.kMeans(x2,2,maxIterations,epsilon)
  val centroids3 = KMeansMatrix.kMeans(x3,3,maxIterations,epsilon)

  //test adequation with the true centroids
  "Trained centroids" - {
    "2 clusters" in {
      for (i <- 0 until 2) {
        trueCentroidsX2.contains(centroids2.row(i).toArray.toList) shouldBe true
      }
    }
    "3 clusters" in {
      for (i <- 0 until 3) {
        trueCentroidsX3.contains(centroids3.row(i).toArray.toList) shouldBe true
      }
    }
  }
}