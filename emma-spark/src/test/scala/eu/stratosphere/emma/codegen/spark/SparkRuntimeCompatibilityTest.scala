package eu.stratosphere.emma.codegen.spark

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.runtime.SparkLocal
import eu.stratosphere.emma.testutil._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class SparkRuntimeCompatibilityTest extends FlatSpec with PropertyChecks with Matchers with BeforeAndAfter {

  var rt: SparkLocal = _

  before {
    rt = SparkLocal("local[1]", 6123)
  }

  after {
    rt.closeSession()
  }

  "LabelRank" should "be usable within a UDF closure" in {
    val sc = rt.sc

    val numbersOfInterest = DataBag(1 to 10).map(_ * 10)

    val res = sc.parallelize(1 to 100).filter(x => numbersOfInterest.exists(_ == x))
    val exp = (1 to 100).filter(x => numbersOfInterest.exists(_ == x))

    compareBags(exp, res.collect())
  }

  "LabelRank" should "be usable as a broadcast variable" in {
    val sc = rt.sc

    val numbersOfInterest = DataBag(1 to 10).map(_ * 10)
    val numbersOfInterestBC = sc.broadcast(numbersOfInterest)

    val res = sc.parallelize(1 to 100).filter(x => numbersOfInterestBC.value.exists(_ == x))
    val exp = (1 to 100).filter(x => numbersOfInterest.exists(_ == x))

    compareBags(exp, res.collect())
  }
}
