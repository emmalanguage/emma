package eu.stratosphere.emma.examples.datamining.classification

/**
 * @author Behrouz DerakhshanS
 */

import eu.stratosphere.emma.testutil._
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class NaiveBayesTest extends FlatSpec with Matchers with BeforeAndAfter {

  "KMeans" should "cluster points around the corners of a hypercube" in withRuntime() { rt =>
    //    val expected = clusters(Source
    //      .fromFile(s"$path/clusters.tsv")
    //      .getLines().map { line =>
    //      val record = line.split(delimiter).map { _.toLong }
    //      record.head -> record(1)
    //    }.toStream)

    val solution = new NaiveBayes("/home/behrouz/Documents/code/emma/emma-examples/data/vote.txt", 1.0, "bernoulli", rt).run()
  }

  }
