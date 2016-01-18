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

  test("Query02") (withRuntime() { rt =>

    // execute with native and with tested environment
    new Query02(inBase, outputPath("q2.tbl.native"), 14, "BRASS", "EUROPE", runtime.Native()).run()
    new Query02(inBase, outputPath("q2.tbl.rt"), 14, "BRASS", "EUROPE", rt).run()

    // compare the results
    val exp = fromPath(outputPath("q2.tbl.native"))
    val res = fromPath(outputPath("q2.tbl.rt"))

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
    //TODO Region: "ASIA" for validation against the qualification database (not enough results in sample data set)
    new Query05(inBase, outputPath("q5.tbl.native"), "AMERICA", "1994-01-01", runtime.Native(), true).run()
    new Query05(inBase, outputPath("q5.tbl.rt"), "AMERICA", "1994-01-01", rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q5.tbl.native"))
    val res = fromPath(outputPath("q5.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  })

  test("Query06") (withRuntime() { rt =>

    // execute with native and with tested environment
    new Query06(inBase, outputPath("q6.tbl.native"), "1994-01-01", 0.06, 24, runtime.Native(), true).run()
    new Query06(inBase, outputPath("q6.tbl.rt"), "1994-01-01", 0.06, 24, rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q6.tbl.native"))
    val res = fromPath(outputPath("q6.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  })

  test("Query07") (withRuntime() { rt =>

    // execute with native and with tested environment
    //TODO Nation1: "FRANCE" for validation against the qualification database (not enough results in sample data set)
    new Query07(inBase, outputPath("q7.tbl.native"), "ARGENTINA", "GERMANY", runtime.Native(), true).run()
    new Query07(inBase, outputPath("q7.tbl.rt"), "ARGENTINA", "GERMANY", rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q7.tbl.native"))
    val res = fromPath(outputPath("q7.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  })

  test("Query08") (withRuntime() { rt =>

    // execute with native and with tested environment
    //TODO Nation: "BRAZIL" for validation against the qualification database (not enough results in sample data set)
    new Query08(inBase, outputPath("q8.tbl.native"), "ETHIOPIA", "AMERICA", "ECONOMY ANODIZED STEEL", runtime.Native(), true).run()
    new Query08(inBase, outputPath("q8.tbl.rt"), "ETHIOPIA", "AMERICA", "ECONOMY ANODIZED STEEL", rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q8.tbl.native"))
    val res = fromPath(outputPath("q8.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  })

  test("Query09") (withRuntime() { rt =>

    // execute with native and with tested environment
    new Query09(inBase, outputPath("q9.tbl.native"), "green", runtime.Native(), true).run()
    new Query09(inBase, outputPath("q9.tbl.rt"), "green", rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q9.tbl.native"))
    val res = fromPath(outputPath("q9.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  })

  test("Query10") (withRuntime() { rt =>

    // execute with native and with tested environment
    new Query10(inBase, outputPath("q10.tbl.native"), "1993-10-01", "1994-01-01", runtime.Native(), true).run()
    new Query10(inBase, outputPath("q10.tbl.rt"), "1993-10-01", "1994-01-01", rt, true).run()

    // compare the results
    val exp = fromPath(outputPath("q10.tbl.native"))
    val res = fromPath(outputPath("q10.tbl.rt"))

    // assert that the result contains the expected values
    compareBags(exp, res)
  })

  def outputPath(suffix: String) = s"$outBase/$suffix"
}
