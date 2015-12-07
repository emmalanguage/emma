package eu.stratosphere.emma.examples.datamining.classification

/**
 * @author Behrouz Derakhshan
 */

import java.io.File

import breeze.linalg.DenseVector
import eu.stratosphere.emma.testutil._
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.io.Source

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class NaiveBayesTest extends FlatSpec with Matchers with BeforeAndAfter {

  val dir = "/classification/naivebayes"
  val path = tempPath(dir)

  before {
    new File(path).mkdirs()
    materializeResource(s"$dir/vote.csv")
    materializeResource(s"$dir/model.txt")
  }

  after {
    deleteRecursive(new File(path))
  }


  "NaiveBayes" should "create the correct model on Bernoulli house-votes-84 data set" in withRuntime() { rt =>
    val expectedModel = Source
      .fromFile(s"$path/model.txt")
      .getLines().map { line =>
      val values = line.split(",").map(_.toDouble).toList
      (values.head, values(1), new DenseVector[Double](values.slice(2, values.size).toArray))
    }.toSeq

    val solution = new NaiveBayes(s"$path/vote.csv", 1.0, "bernoulli", rt).algorithm.run(rt).fetch()

    solution should equal(expectedModel)
  }
}
