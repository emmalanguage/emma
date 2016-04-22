package eu.stratosphere.emma.examples.lara

import eu.stratosphere.emma.api.lara.{Matrix, Vector}

import scala.util.Random

/** Linear Regression example
  * Computes simple linear regression with batch gradient descent.
  * The data can be generated in two different ways given the API.
  */
object LinearRegressionGD {

  def gradientDescent(X: Matrix[Double], y: Vector[Double], betas: Vector[Double],
                      alpha: Double, numIterations: Int): Vector[Double] = {

    val m = X.numRows
    var newBetas = betas

    for (i <- 0 until numIterations) {
      val hypothesis = X %*% newBetas // compute current hypothesis/activations
      val loss       = hypothesis - y // compute loss
      val cost       = loss.map(x => x * x).aggregate((a, b) => a + b) / (2 * m) // compute avg cost per example
      val gradient   = (X.transpose() %*% loss) / m // compute new gradient

      // update parameters
      newBetas = newBetas - (gradient * alpha)
    }

    // return the trained model
    newBetas
  }

  /** Generate training data with labels/target values
    *
    * @param numPoints number of datapoints to generate, shape of resulting matrix x will be (numPoints,2)
    * @param bias the bias of the target values
    * @param variance variance in the target labels (noise)
    * @return pair of training data and labels/target values
    */
  def genData(numPoints: Int, bias: Double, variance: Double): (Matrix[Double], Vector[Double]) = {
    /*

    // generate data in databag comprehension style

    val m = for (i <- x.rRange; j <- x.cRange) yield {
      if (j == 0) (i, j, 1)
      else        (i, j, i)
    }

    // generate data with cell assignments

    val x = Matrix.zeros[Double](numPoints, 2)
    val y = Vector.zeros[Double](numPoints)

    for (i <- x.rRange /* ; j <- x.cRange */) {
      x(i, 0) = 1
      x(i, 1) = i
    }

    */

    // generate data with fill

    val x = Matrix.fill[Double](numPoints, 2)((i,j) => if (j == 0) 1 else i)
    val y = Vector.fill[Double](numPoints)(i => (i + bias) + Random.nextDouble() * variance)

    (x, y)
  }
}
