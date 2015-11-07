package eu.stratosphere.emma.codegen.spark

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.runtime.Spark
import eu.stratosphere.emma.testutil._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._

@RunWith(classOf[JUnitRunner])
class SparkRuntimeCompatibilityTest extends FlatSpec with Matchers {
  val cores = Runtime.getRuntime.availableProcessors
  def engine = new Spark()

  "DataBag" should "be usable within a UDF closure" in withRuntime(engine) { case spark: Spark =>
    val ofInterest = DataBag(1 to 10) map (_ * 10)
    val actual = spark.sc.parallelize(1 to 100) filter { x => ofInterest exists (_ == x) }
    val expected = 1 to 100 filter { x => ofInterest exists (_ == x) }
    compareBags(expected, actual.collect())
  }

  it should "be usable as a broadcast variable" in withRuntime(engine) { case spark: Spark =>
    val ofInterest = DataBag(1 to 10) map (_ * 10)
    val ofInterestBC = spark.sc.broadcast(ofInterest)

    val actual = spark.sc.parallelize(1 to 100)
      .filter { x => ofInterestBC.value exists (_ == x) }

    val expected = 1 to 100 filter { x => ofInterest exists (_ == x) }
    compareBags(expected, actual.collect())
  }
}
