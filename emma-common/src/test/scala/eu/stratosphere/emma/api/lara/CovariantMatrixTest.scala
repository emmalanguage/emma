package eu.stratosphere.emma.api.lara

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CovariantMatrixTest extends BaseTest {
  // scalastyle:off
  val Result = Array.apply(
    0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
    0.0, 9.166666666666666, 18.333333333333332, 27.5, 36.666666666666664, 45.833333333333336, 55.0, 64.16666666666667, 73.33333333333333, 82.5,
    0.0, 18.333333333333332, 36.666666666666664, 55.0, 73.33333333333333, 91.66666666666667, 110.0, 128.33333333333334, 146.66666666666666, 165.0,
    0.0, 27.5, 55.0, 82.5, 110.0, 137.5, 165.0, 192.5, 220.0, 247.5,
    0.0, 36.666666666666664, 73.33333333333333, 110.0, 146.66666666666666, 183.33333333333334, 220.0, 256.6666666666667, 293.3333333333333, 330.0,
    0.0, 45.833333333333336, 91.66666666666667, 137.5, 183.33333333333334, 229.16666666666666, 275.0, 320.8333333333333, 366.6666666666667, 412.5,
    0.0, 55.0, 110.0, 165.0, 220.0, 275.0, 330.0, 385.0, 440.0, 495.0,
    0.0, 64.16666666666667, 128.33333333333334, 192.5, 256.6666666666667, 320.8333333333333, 385.0, 449.1666666666667, 513.3333333333334, 577.5,
    0.0, 73.33333333333333, 146.66666666666666, 220.0, 293.3333333333333, 366.6666666666667, 440.0, 513.3333333333334, 586.6666666666666, 660.0,
    0.0, 82.5, 165.0, 247.5, 330.0, 412.5, 495.0, 577.5, 660.0, 742.5
  )
  // scalastyle:on

  "Covariance Matrix" in {

    val M = Matrix.fill[Double](10, 10)((i, j) => i * j)

    //     TODO: Here we actually want means to be a vector instead of a Traversable
    //     The result somehow depends as we ant to have a vector in this case here
    //     but in general the result shoud be a matrix
    //     return a matrix 1 x n instead?
    val means = for (col <- M.cols()) yield {
      col.aggregate(_ + _) / col.length
    }
    val meanVector = Vector.apply[Double](means.toArray)
    val U = M - Matrix.fill[Double](M.numRows, M.numCols)((i, j) => meanVector.get(j))

    //     TODO: We can not write 1 / ... * U // have to provide implicits
    val C = U.transpose() %*% U * 1 / (U.numRows - 1)

    C.toArray should be(Result)
  }
}
