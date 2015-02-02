package eu.stratosphere.emma.codegen.flink

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.codegen.flink.TestSchema._
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.runtime.Engine
import eu.stratosphere.emma.testutil._
import org.junit.{After, Before, Test}

import java.io.File
import scala.reflect.runtime.universe._

object CodegenTest {

  /**
   * Temporary, only for debugging.
   *
   */
  def main(args: Array[String]): Unit = {
    val test = new CodegenTest()
    test.setup()
    test.testCSVReadWriteComplexType()
    test.teardown()
  }
}

class CodegenTest {

  var rt: Engine = _

  var inBase = tempOutputPath("test/input")
  var outBase = tempOutputPath("test/output")

  @Before def setup() {
    // create a new runtime session
    rt = runtime.factory("flink-local", "localhost", 6123)
    // make sure that the base paths exist
    new File(inBase).mkdirs()
    new File(outBase).mkdirs()
  }

  @After def teardown(): Unit = {
    // close the runtime session
    rt.closeSession()
  }

  // --------------------------------------------------------------------------
  // Scatter / Gather
  // --------------------------------------------------------------------------

  @Test def testScatterGatherSimpleType(): Unit = {
    testScatterGather(Seq(2, 4, 6, 8, 10))
  }

  @Test def testScatterGatherComplexType(): Unit = {
    testScatterGather(Seq(
      EdgeWithLabel(1L, 4L, Label(0.5, "A", z = false)),
      EdgeWithLabel(2L, 5L, Label(0.8, "B", z = true)),
      EdgeWithLabel(3L, 6L, Label(0.2, "C", z = false))))
  }

  private def testScatterGather[A: TypeTag](inp: Seq[A]): Unit = {
    // scatter the input bag
    val sct = rt.scatter(inp)

    // assert that the scattered bag contains the input values
    compareBags(inp, sct.fetch())

    // repeat three times to test the memo underlying implementation
    for (i <- 0 until 3) {
      // gather back the scattered values
      val res = rt.gather(sct)
      // assert that the result contains the input values
      compareBags(inp, res.fetch())
    }
  }

  // --------------------------------------------------------------------------
  // CSV I/O
  // --------------------------------------------------------------------------

  @Test def testCSVReadWriteComplexType(): Unit = {
    testCSVReadWrite[EdgeWithLabel[Long, String]](Seq(
      EdgeWithLabel(1L, 4L, "A"),
      EdgeWithLabel(2L, 5L, "B"),
      EdgeWithLabel(3L, 6L, "C")))
  }

  private def testCSVReadWrite[A: TypeTag : CSVConvertors](inp: Seq[A]): Unit = {
    // construct a parameterized algorithm family
    val alg = (suffix: String) => emma.parallelize {
      val outputPath = s"$outBase/csv.$suffix"
      // write out the original input
      write(outputPath, new CSVOutputFormat[A])(DataBag(inp))
      // return the output path
      outputPath
    }

    // write the input to a file using the original code and the runtime under test
    val actPath = alg("native").run(runtime.Native)
    val resPath = alg("flink").run(rt)

    val exp = scala.io.Source.fromFile(resPath).getLines().toStream
    val res = scala.io.Source.fromFile(actPath).getLines().toStream

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  // --------------------------------------------------------------------------
  // Map
  // --------------------------------------------------------------------------

  @Test def testMapSimpleType(): Unit = {
    val inp = Seq(2, 4, 6, 8, 10)

    // define some closure parameters
    val denominator = 4.0
    val offset = 15.0

    val alg = emma.parallelize {
      for (x <- DataBag(inp)) yield offset + x / denominator
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch()
    val exp = alg.run(rt).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testMapComplexType(): Unit = {
    val inp = Seq(
      EdgeWithLabel(1L, 4L, Label(0.5, "A", z = false)),
      EdgeWithLabel(2L, 5L, Label(0.8, "B", z = true)),
      EdgeWithLabel(3L, 6L, Label(0.2, "A", z = false)))

    val y = "A"

    val alg = emma.parallelize {
      for (e <- DataBag(inp)) yield EdgeWithLabel(e.dst, e.src, e.label.y == y)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch()
    val exp = alg.run(rt).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  // --------------------------------------------------------------------------
  // FlatMap
  // --------------------------------------------------------------------------

  @Test def testFlatMapSimpleType(): Unit = {
    val inp = scala.io.Source.fromFile(materializeResource("/lyrics/Jabberwocky.txt")).getLines().toStream

    val len = 3

    val alg = emma.parallelize {
      DataBag(inp).flatMap(x => DataBag(x.split("\\W+").filter(_.length > len)))
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch()
    val exp = alg.run(rt).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }
}
