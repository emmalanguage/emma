package eu.stratosphere.emma.examples.tpch

import java.io.File

import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.testutil._
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class TPCHTest extends FunSuite with Matchers with BeforeAndAfterAll {

  var inBase = tempPath("/tpch/sf0.001")
  var outBase = tempPath("/tpch/sf0.001/output")

  override def beforeAll() = {
    // make sure that the base paths exist
    new File(inBase).mkdirs()
    new File(outBase).mkdirs()
    // materialize the TPCH schema
    materializeResource("/tpch/sf0.001/customer.tbl")
    materializeResource("/tpch/sf0.001/lineitem.tbl")
    materializeResource("/tpch/sf0.001/nation.tbl")
    materializeResource("/tpch/sf0.001/orders.tbl")
    materializeResource("/tpch/sf0.001/part.tbl")
    materializeResource("/tpch/sf0.001/partsupp.tbl")
    materializeResource("/tpch/sf0.001/region.tbl")
    materializeResource("/tpch/sf0.001/supplier.tbl")
  }

  override def afterAll() = {
    deleteRecursive(new File(outBase))
    deleteRecursive(new File(inBase))
  }

  test("Query01") (withRuntime() { rt =>

    // execute with native and with tested environment
    new Query01(inBase, outputPath("q1.tbl.native"), 30, runtime.Native(), true).run()
    new Query01(inBase, outputPath("q1.tbl.rt"), 30, rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q1.tbl.native"))
    val res = fromPath(outputPath("q1.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  })

  test("Query03") (withRuntime() { rt =>

    // execute with native and with tested environment
    new Query03(inBase, outputPath("q3.tbl.native"), "AUTOMOBILE", "1996-06-30", runtime.Native(), true).run()
    new Query03(inBase, outputPath("q3.tbl.rt"), "AUTOMOBILE", "1996-06-30", rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q1.tbl.native"))
    val res = fromPath(outputPath("q1.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  })

  test("Query05") (withRuntime() { rt =>

    // execute with native and with tested environment
    new Query05(inBase, outputPath("q5.tbl.native"), "AMERICA", "1994-01-01", runtime.Native(), true).run()
    new Query05(inBase, outputPath("q5.tbl.rt"), "AMERICA", "1994-01-01", rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q5.tbl.native"))
    val res = fromPath(outputPath("q5.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  })

  def outputPath(suffix: String) = s"$outBase/$suffix"
}
