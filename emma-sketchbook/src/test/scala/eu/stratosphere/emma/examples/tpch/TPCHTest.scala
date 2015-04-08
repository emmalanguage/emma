package eu.stratosphere.emma.examples.tpch

import java.io.File

import eu.stratosphere.emma.examples.tpch.query01.Query01
import eu.stratosphere.emma.examples.tpch.query03.Query03
import eu.stratosphere.emma.examples.tpch.query05.Query05
import eu.stratosphere.emma.examples.tpch.query06.Query06
import eu.stratosphere.emma.examples.tpch.query07.Query07
import eu.stratosphere.emma.examples.tpch.query08.Query08
import eu.stratosphere.emma.examples.tpch.query09.Query09
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.runtime.Engine
import eu.stratosphere.emma.testutil._
import org.junit.{Before, Test}

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

  @Test def testQuery01(): Unit = {

    // execute with native and with tested environment
    new Query01(inBase, outputPath("q1.tbl.native"), 30, runtime.Native(), true).run()
    new Query01(inBase, outputPath("q1.tbl.flink"), 30, rt, true).run()

    // compare the results
    val exp = scala.io.Source.fromFile(outputPath("q1.tbl.native")).getLines().toStream.toList.sorted
    val res = (1 to rt.defaultDOP flatMap (i => scala.io.Source.fromFile(outputPath(s"q1.tbl.flink/$i")).getLines().toStream)).toList.sorted

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  @Test def testQuery03(): Unit = {

    // execute with native and with tested environment
    new Query03(inBase, outputPath("q3.tbl.native"), "AUTOMOBILE", "1996-06-30", runtime.Native(), true).run()
    new Query03(inBase, outputPath("q3.tbl.flink"), "AUTOMOBILE", "1996-06-30", rt, true).run()

    // compare the results
    val exp = scala.io.Source.fromFile(outputPath("q3.tbl.native")).getLines().toStream.toList.sorted
    val res = (1 to rt.defaultDOP flatMap (i => scala.io.Source.fromFile(outputPath(s"q3.tbl.flink/$i")).getLines().toStream)).toList.sorted

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  @Test def testQuery05(): Unit = {

    // execute with native and with tested environment
    //TODO Region: "ASIA" for validation against the qualification database (not enough results in sample data set)
    new Query05(inBase, outputPath("q5.tbl.native"), "AMERICA", "1994-01-01", runtime.Native(), true).run()
    new Query05(inBase, outputPath("q5.tbl.flink"), "AMERICA", "1994-01-01", rt, true).run()

    // compare the results
    val exp = scala.io.Source.fromFile(outputPath("q5.tbl.native")).getLines().toStream.toList.sorted
    val res = (1 to rt.defaultDOP flatMap (i => scala.io.Source.fromFile(outputPath(s"q5.tbl.flink/$i")).getLines().toStream)).toList.sorted

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  @Test def testQuery06(): Unit = {

    // execute with native and with tested environment
    new Query06(inBase, outputPath("q6.tbl.native"), "1994-01-01", 0.06, 24, runtime.Native(), true).run()
    new Query06(inBase, outputPath("q6.tbl.flink"), "1994-01-01", 0.06, 24, rt, true).run()

    // compare the results
    val exp = scala.io.Source.fromFile(outputPath("q6.tbl.native")).getLines().toStream.toList.sorted
    val res = (1 to rt.defaultDOP flatMap (i => scala.io.Source.fromFile(outputPath(s"q6.tbl.flink/$i")).getLines().toStream)).toList.sorted

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  @Test def testQuery07(): Unit = {

    // execute with native and with tested environment
    //TODO Nation1: "FRANCE" for validation against the qualification database (not enough results in sample data set)
    new Query07(inBase, outputPath("q7.tbl.native"), "ARGENTINA", "GERMANY", runtime.Native(), true).run()
    new Query07(inBase, outputPath("q7.tbl.flink"), "ARGENTINA", "GERMANY", rt, true).run()

    // compare the results
    val exp = scala.io.Source.fromFile(outputPath("q7.tbl.native")).getLines().toStream.toList.sorted
    val res = (1 to rt.defaultDOP flatMap (i => scala.io.Source.fromFile(outputPath(s"q7.tbl.flink/$i")).getLines().toStream)).toList.sorted

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  @Test def testQuery08(): Unit = {

    // execute with native and with tested environment
    //TODO Nation: "BRAZIL" for validation against the qualification database (not enough results in sample data set)
    new Query08(inBase, outputPath("q8.tbl.native"), "ETHIOPIA", "AMERICA", "ECONOMY ANODIZED STEEL", runtime.Native(), true).run()
    new Query08(inBase, outputPath("q8.tbl.flink"), "ETHIOPIA", "AMERICA", "ECONOMY ANODIZED STEEL", rt, true).run()

    // compare the results
    val exp = scala.io.Source.fromFile(outputPath("q8.tbl.native")).getLines().toStream.toList.sorted
    val res = (1 to rt.defaultDOP flatMap (i => scala.io.Source.fromFile(outputPath(s"q8.tbl.flink/$i")).getLines().toStream)).toList.sorted

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  @Test def testQuery09(): Unit = {

    // execute with native and with tested environment
    new Query09(inBase, outputPath("q9.tbl.native"), "green", runtime.Native(), true).run()
    new Query09(inBase, outputPath("q9.tbl.flink"), "green", rt, true).run()

    // compare the results
    val exp = scala.io.Source.fromFile(outputPath("q9.tbl.native")).getLines().toStream.toList.sorted
    val res = (1 to rt.defaultDOP flatMap (i => scala.io.Source.fromFile(outputPath(s"q9.tbl.flink/$i")).getLines().toStream)).toList.sorted

    // assert that the result contains the expected values
    compareBags(exp, res)
  }
  def outputPath(suffix: String) = s"$outBase/$suffix"
}
