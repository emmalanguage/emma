package eu.stratosphere.emma.codegen.flink

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.codegen.flink.TestSchema.{Edge, FilmFestivalWinner}
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.runtime.Engine
import eu.stratosphere.emma.testutil._
import org.junit.{After, Before, Test}

object CodegenTest {

  /**
   * Temporary, only for debugging.
   *
   */
  def main(args: Array[String]): Unit = {
    val test = new CodegenTest()
    test.setup()
    test.testScatterMapGatherSimpleType()
    test.teardown()
  }
}

class CodegenTest {

  var rt: Engine = _

  @Before def setup() {
    rt = runtime.factory("flink-local", "localhost", 6123)
  }

  @After def teardown(): Unit = {
    rt.closeSession()
  }

  @Test def testScatterMapGatherSimpleType(): Unit = {
    val inp = Seq(2, 4, 6, 8, 10)

    val algorithm = emma.parallelize {
      val I = DataBag(inp)
      for (x <- I) yield 2 * x
    }

    val exp = for (x <- inp) yield 2 * x
    val res = algorithm.run(rt).fetch()

    assert((exp diff res) == Seq.empty[Int], s"Unexpected elements in result: $exp")
    assert((res diff exp) == Seq.empty[Int], s"Unseen elements in result: $res")
  }

  @Test def testScatterMapGatherComplexType(): Unit = {
    val inp = Seq(Edge(1, 2))

    val algorithm = emma.parallelize {
      for (x <- DataBag(inp)) yield Edge(2 * x.src, 5 * x.dst)
    }

    val exp = for (x <- inp) yield Edge(2 * x.src, 5 * x.dst)
    val res = algorithm.run(rt).fetch()

    assert((exp diff res) == Seq.empty[Edge[Int]], s"Unexpected elements in result: $exp")
    assert((res diff exp) == Seq.empty[Edge[Int]], s"Unseen elements in result: $res")
  }

  @Test def testReadCSVMapGather(): Unit = {
    val algorithm = emma.parallelize {
      val winners = read(materializeResource("/cinema/berlinalewinners.csv"), new CSVInputFormat[FilmFestivalWinner])
      for (f <- winners) yield f.country
    }

    val res = algorithm.run(rt).fetch()
    val exp = algorithm.run(runtime.Native).fetch()

    assert((exp diff res) == Seq.empty[String], s"Unexpected elements in result: $exp")
    assert((res diff exp) == Seq.empty[String], s"Unseen elements in result: $res")
  }
}
