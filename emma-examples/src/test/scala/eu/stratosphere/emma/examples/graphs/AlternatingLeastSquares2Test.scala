package eu.stratosphere.emma.examples.graphs

import java.io.File

import breeze.linalg._
import breeze.stats.distributions._
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.testutil._
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalacheck.Gen._
import org.scalacheck.{Arbitrary => Arb}
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class AlternatingLeastSquares2Test extends FunSuite with PropertyChecks with Matchers {

  import AlternatingLeastSquares2.Schema._
  import AlternatingLeastSquares2Test._

  // default parameters
  val path       = tempPath("/graphs/als")
  val features   = 8
  val lambda     = 0.065
  val iterations = 10
  val rt         = runtime.default()

  // initialize resources
  new File(path).mkdirs()
  materializeResource(s"/graphs/als/r.base")
  materializeResource(s"/graphs/als/r.test")

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
      iterations: Int) = {
    val base = s"$path/r.base"
    val test = s"$path/r.test"
    val out  = s"$path/output"
    val alg  = new AlternatingLeastSquares2(
      base, out, features, lambda, iterations, rt).algorithm

    val result   = alg.run(rt).fetch().iterator
    val expected = readRatings(test)
    FileUtils.deleteQuietly(new File(s"$path/output"))
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
