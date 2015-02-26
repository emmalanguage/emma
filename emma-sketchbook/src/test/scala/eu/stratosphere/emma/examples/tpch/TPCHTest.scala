package eu.stratosphere.emma.examples.tpch

import java.io.File

import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.runtime.Engine
import eu.stratosphere.emma.testutil._
import org.junit.{Ignore, Test, After, Before}

class TPCHTest {

  var rt: Engine = _

  var inBase = tempPath("/tpch/sf0.001")
  var outBase = tempPath("/tpch/sf0.001/output")

  @Before def setup() {
    // create a new runtime session
    rt = runtime.factory("flink-local", "localhost", 6123)

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

  @After def teardown(): Unit = {
    // close the runtime session
    rt.closeSession()
  }

  @Ignore
  @Test def testQuery01(): Unit = {

    // execute with native and with tested environment
    new Query01(inBase, outputPath("q1.tbl.native"), 30, runtime.Native).run()
    new Query01(inBase, outputPath("q1.tbl.flink"), 30, rt).run()

    // compare the results
    val exp = scala.io.Source.fromFile(outputPath("q1.tbl.flink")).getLines().toStream
    val res = scala.io.Source.fromFile(outputPath("q1.tbl.flink")).getLines().toStream

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  @Ignore
  @Test def testQuery03(): Unit = {

    // execute with native and with tested environment
    // new Query03(inBase, outputPath("q3.tbl.native"), "", "", runtime.Native).run()
    new Query03(inBase, outputPath("q3.tbl.flink"), "", "", rt).run()

    // compare the results
    //    val exp = scala.io.Source.fromFile(outputPath("q3.tbl.native")).getLines().toStream
    //    val res = scala.io.Source.fromFile(outputPath("q3.tbl.flink")).getLines().toStream

    // assert that the result contains the expected values
    //    compareBags(exp, res)
  }

  def outputPath(suffix: String) = s"$outBase/$suffix"
}
