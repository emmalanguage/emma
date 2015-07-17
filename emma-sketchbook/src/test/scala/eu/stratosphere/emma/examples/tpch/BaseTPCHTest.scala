package eu.stratosphere.emma.examples.tpch

import java.io.File

import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.runtime.Engine
import eu.stratosphere.emma.testutil._
import org.junit.{After, Before, Test}

abstract class BaseTPCHTest(rtName: String) {

  var rt: Engine = _

  var inBase = tempPath("/tpch/sf0.001")
  var outBase = tempPath("/tpch/sf0.001/output")

  protected def runtimeUnderTest: runtime.Engine

  @Before def setup() {
    // create a new runtime session
    rt = runtimeUnderTest

    // materialize the TPCH schema
    materializeResource("/tpch/sf0.001/customer.tbl")
    materializeResource("/tpch/sf0.001/lineitem.tbl")
    materializeResource("/tpch/sf0.001/nation.tbl")
    materializeResource("/tpch/sf0.001/orders.tbl")
    materializeResource("/tpch/sf0.001/part.tbl")
    materializeResource("/tpch/sf0.001/partsupp.tbl")
    materializeResource("/tpch/sf0.001/region.tbl")
    materializeResource("/tpch/sf0.001/supplier.tbl")

    // make sure that the base paths exist
    new File(outBase).mkdirs()
    new File(outBase).mkdirs()
  }

  @After def shutdown(): Unit = {
    rt.closeSession()
  }

  @Test def testQuery01(): Unit = {

    // execute with native and with tested environment
    new Query01(inBase, outputPath("q1.tbl.native"), 30, runtime.Native(), true).run()
    new Query01(inBase, outputPath("q1.tbl.rt"), 30, rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q1.tbl.native"))
    val res = fromPath(outputPath("q1.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  @Test def testQuery03(): Unit = {

    // execute with native and with tested environment
    new Query03(inBase, outputPath("q3.tbl.native"), "AUTOMOBILE", "1996-06-30", runtime.Native(), true).run()
    new Query03(inBase, outputPath("q3.tbl.rt"), "AUTOMOBILE", "1996-06-30", rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q1.tbl.native"))
    val res = fromPath(outputPath("q1.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  @Test def testQuery05(): Unit = {

    // execute with native and with tested environment
    new Query05(inBase, outputPath("q5.tbl.native"), "AMERICA", "1994-01-01", runtime.Native(), true).run()
    new Query05(inBase, outputPath("q5.tbl.rt"), "AMERICA", "1994-01-01", rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q5.tbl.native"))
    val res = fromPath(outputPath("q5.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  def outputPath(suffix: String) = s"$outBase/$suffix"
}
