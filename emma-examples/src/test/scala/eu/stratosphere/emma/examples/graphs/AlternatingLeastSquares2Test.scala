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
package eu.stratosphere.emma.examples.graphs

import breeze.linalg._
import breeze.stats.distributions._
import eu.stratosphere.emma.testutil._
import java.io.File
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalacheck.Gen._
import org.scalacheck.{Arbitrary => Arb}
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks
import scala.io.Source

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class AlternatingLeastSquares2Test extends FunSuite
    with PropertyChecks with Matchers with BeforeAndAfter {

  import AlternatingLeastSquares2._
  import AlternatingLeastSquares2Test._

  // default parameters
  val path       = tempPath("/graphs/als")
  val features   = 8
  val lambda     = 0.065
  val iterations = 10

  before {
    new File(path).mkdirs()
    materializeResource(s"/graphs/als/r.base")
    materializeResource(s"/graphs/als/r.test")
  }

  after {
    deleteRecursive(new File(path))
  }

  test("M[i][j] * (M^t)[j][i] = sum[j](M[i][*] * M[i][*]^t)") {
    forAll { M: DenseMatrix[Int] =>
      val Ms  = for (j <- 0 until M.cols; col = M(::, j)) yield col * col.t
      val MMt = (DenseMatrix.zeros[Int](M.rows, M.rows) /: Ms) { _ + _ }
      MMt should equal (M * M.t)
    }
  }

  test("ALS with increasing number of features") {
    val errors = for (features <- 5 to 15 by 5) yield {
      val err  = validate(path, features, lambda, iterations)
      println(f"$features%2d features: $err")
      err
    }

    errors should equal (errors.sorted.reverse)
  }

  test("ALS with increasing number of iterations") {
    val errors = for (iterations <- 5 to 15 by 5) yield {
      val err  = validate(path, features, lambda, iterations)
      println(f"$iterations%2d iterations: $err")
      err
    }

    errors should equal (errors.sorted.reverse)
  }

  /** Test the Alternating Least Squares algorithm with given parameters. */
  def validate(
      path:       String,
      features:   Int,
      lambda:     Double,
      iterations: Int) = withRuntime() { rt =>
    val base = s"$path/r.base"
    val test = s"$path/r.test"
    val out  = s"$path/output"
    val alg  = new AlternatingLeastSquares2(
      base, out, features, lambda, iterations, rt).algorithm

    val result   = alg.run(rt).fetch().iterator
    val expected = readRatings(test)
    rmse(expected, result)
  }

  /** Read a sequence of Ratings. */
  def readRatings(path: String) = for {
    line <- Source.fromFile(path).getLines()
    split = line.split('\t')
    if split.length >= 3
    Array(u, i, r, _*) = split
  } yield Rating(u.toInt, i.toInt, r.toDouble)

  /** Calculate the Root-Mean-Square Error between expected and actual. */
  def rmse(expected: Iterator[Rating], actual: Iterator[Rating]) = {
    def ratingMap(ratings: Iterator[Rating]) = {
      for (Rating(u, i, r) <- ratings) yield u -> i -> r
    }.toMap.withDefaultValue(0.0)

    val exp = ratingMap(expected)
    val act = ratingMap(actual)

    math.sqrt({ for (k <- exp.keys)
      yield math.pow(exp(k) - act(k), 2)
    }.sum / (exp.size min act.size))
  }
}

object AlternatingLeastSquares2Test {
  def arb[A: Arb] = implicitly[Arb[A]].arbitrary

  implicit val uniformIntMatrix:    Arb[DenseMatrix[Int]] =
    Arb(for (m <- posNum[Int]; n <- posNum[Int])
      yield DenseMatrix.rand(m, n, Rand.randInt))

  implicit val uniformDoubleMatrix: Arb[DenseMatrix[Double]] =
    Arb(for (m <- posNum[Int]; n <- posNum[Int])
      yield DenseMatrix.rand(m, n))
}
