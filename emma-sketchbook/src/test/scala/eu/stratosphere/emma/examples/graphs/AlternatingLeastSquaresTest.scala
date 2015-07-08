package eu.stratosphere.emma.examples.graphs

import java.io.File

import breeze.linalg._
import breeze.stats.distributions._
import eu.stratosphere.emma.runtime._
import eu.stratosphere.emma.testutil._
import org.junit.runner.RunWith
import org.scalacheck.Gen._
import org.scalacheck.{Arbitrary => Arb}
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class AlternatingLeastSquaresTest extends FunSuite with PropertyChecks with Matchers {

  import AlternatingLeastSquares.Schema._
  import AlternatingLeastSquaresTest._

  // default parameters
  val path       = tempPath("/graphs/als")
  val features   = 8
  val lambda     = 0.065
  val iterations = 25
  val rt         = Native()
  val dataSets   = 1 to 5

  // initialize resources
  new File(path).mkdirs()
  for (i <- dataSets) {
    materializeResource(s"/graphs/als/r$i.base")
    materializeResource(s"/graphs/als/r$i.test")
  }

  test("M[i][j] * (M^t)[j][i] = sum[j](M[i][*] * M[i][*]^t)") {
    forAll { M: DenseMatrix[Int] =>
      val Ms  = for (j <- 0 until M.cols; col = M(::, j)) yield col * col.t
      val MMt = (DenseMatrix.zeros[Int](M.rows, M.rows) /: Ms) { _ + _ }
      MMt should equal (M * M.t)
    }
  }

  test("ALS with increasing number of features") {
    val errors = for (features <- 5 to 25 by 5) yield {
      val err  = crossValidate(path, features, lambda, iterations, rt)
      println(f"$features%2d features: $err")
      err
    }

    errors should equal (errors.sorted.reverse)
  }

  test("ALS with increasing number of iterations") {
    val errors = for (iterations <- 10 to 50 by 10) yield {
      val err  = crossValidate(path, features, lambda, iterations, rt)
      println(f"$iterations%2d iterations: $err")
      err
    }

    errors should equal (errors.sorted.reverse)
  }

  /** Test the Alternating Least Squares algorithm with given parameters. */
  def crossValidate(
      path:       String,
      features:   Int,
      lambda:     Double,
      iterations: Int,
      rt:         Engine) =
    (for (i <- dataSets) yield {
      val base = s"$path/r$i.base"
      val test = s"$path/r$i.test"
      val out  = s"$path/output"
      new AlternatingLeastSquares(
        base, out, features, lambda, iterations, rt).run()
      val expected = readRatings(test)
      val result   = readRatings(out)
      rmse(expected, result)
    }).sum / dataSets.size

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

object AlternatingLeastSquaresTest {
  def arb[A: Arb] = implicitly[Arb[A]].arbitrary

  implicit val uniformIntMatrix:    Arb[DenseMatrix[Int]] =
    Arb(for (m <- posNum[Int]; n <- posNum[Int])
      yield DenseMatrix.rand(m, n, Rand.randInt))

  implicit val uniformDoubleMatrix: Arb[DenseMatrix[Double]] =
    Arb(for (m <- posNum[Int]; n <- posNum[Int])
      yield DenseMatrix.rand(m, n))
}
