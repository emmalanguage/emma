package eu.stratosphere.emma.codegen.flink

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.runtime.FlinkLocal
import eu.stratosphere.emma.testutil._
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.configuration.Configuration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class FlinkRuntimeCompatibilityTest extends FlatSpec with PropertyChecks with Matchers with BeforeAndAfter {

  import org.apache.flink.api.scala._

  var rt: FlinkLocal = _

  before {
    rt = FlinkLocal("localhost", 6123)
  }

  after {
    rt.closeSession()
  }

  "LabelRank" should "be usable within a UDF closure" in {
    val env = rt.env

    val numbersOfInterest = DataBag(1 to 10).map(_ * 10)

    val res = env.fromCollection(1 to 100).filter(x => numbersOfInterest.exists(_ == x))
    val exp = (1 to 100).filter(x => numbersOfInterest.exists(_ == x))

    compareBags(exp, res.collect())
  }

  "LabelRank" should "be usable as a broadcast variable" in {
    val env = rt.env

    val numbersOfInterest = DataBag(1 to 10).map(_ * 10)

    val res = env.fromCollection(1 to 100).filter(new RichFilterFunction[Int] {
      import scala.collection.JavaConverters._
      var numbersOfInterest: DataBag[Int] = null

      override def open(parameters: Configuration) = {
        this.numbersOfInterest = DataBag(getRuntimeContext.getBroadcastVariable("numbersOfInterest").asScala);
      }

      override def filter(x: Int): Boolean = numbersOfInterest.exists(_ == x)
    }).withBroadcastSet(env.fromCollection(numbersOfInterest.fetch()), "numbersOfInterest")
    val exp = (1 to 100).filter(x => numbersOfInterest.exists(_ == x))

    compareBags(exp, res.collect())
  }
}
