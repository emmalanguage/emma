package eu.stratosphere.emma.examples.lara

import eu.stratosphere.emma.api.lara.BaseTest
import eu.stratosphere.emma.api.lara.{Vector, Matrix}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LinearRegressionGDTest extends BaseTest{

  "Linear Regression with Batch Gradient Descent" in {

    val (x, y)        = LinearRegressionGD.genData(100, 25, 10)
    val m             = x.numRows
    val n             = x.numCols
    val numIterations = 100000
    val alpha         = 0.0005
    val betas         = Vector.ones[Double](n)

    val result = LinearRegressionGD.gradientDescent(x, y, betas, alpha, numIterations)
    println(result)

    val b0Diff = math.abs(result.get(0) - 30.0)
    val b1Diff = math.abs(result.get(1) - 1)

    assert(b0Diff < 1.0)
    assert(b1Diff < 0.1)
  }
}
