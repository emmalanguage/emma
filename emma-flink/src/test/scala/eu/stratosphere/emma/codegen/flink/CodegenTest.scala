package eu.stratosphere.emma.codegen.flink

import java.io.File

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.codegen.flink.TestSchema._
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.runtime.Engine
import eu.stratosphere.emma.testutil._
import org.junit.{After, Before, Test}

import scala.reflect.runtime.universe._

class CodegenTest {

  var rt: Engine = _

  var inBase = tempPath("test/input")
  var outBase = tempPath("test/output")

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

  @Test def testFlatMap(): Unit = {
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

  // --------------------------------------------------------------------------
  // Filter
  // --------------------------------------------------------------------------

  @Test def testFilter(): Unit = {
    val inp = scala.io.Source.fromFile(materializeResource("/lyrics/Jabberwocky.txt")).getLines().toStream

    val len = 10

    val alg = emma.parallelize {
      DataBag(inp).withFilter(_.length > len)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch()
    val exp = alg.run(rt).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  // --------------------------------------------------------------------------
  // Distinct and Union
  // --------------------------------------------------------------------------

  @Test def testDistinctSimpleType(): Unit = {
    val inp = {
      val lines = scala.io.Source.fromFile(materializeResource("/lyrics/Jabberwocky.txt")).getLines()
      lines.flatMap(_.split("\\W+")).toStream
    }

    val alg = emma.parallelize {
      DataBag(inp).distinct()
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch()
    val exp = alg.run(rt).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testDistinctComplexType(): Unit = {
    val inp = {
      val lines = scala.io.Source.fromFile(materializeResource("/lyrics/Jabberwocky.txt")).getLines()
      lines.flatMap(_.split("\\W+")).zipWithIndex.toStream
    }

    val alg = emma.parallelize {
      DataBag(inp).distinct()
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch()
    val exp = alg.run(rt).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testUnion(): Unit = {
    val inp = {
      val lines = scala.io.Source.fromFile(materializeResource("/lyrics/Jabberwocky.txt")).getLines()
      lines.flatMap(_.split("\\W+")).toStream
    }

    val lft = inp.filter(_.length % 2 == 0) // even-length words
    val rgt = inp.filter(_.length % 2 == 1) // odd-length words

    val alg = emma.parallelize {
      DataBag(lft) plus DataBag(rgt)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch()
    val exp = alg.run(rt).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  // --------------------------------------------------------------------------
  // Join & Cross
  // --------------------------------------------------------------------------

  @Test def testTwoWayJoinSimpleType(): Unit = {
    val N = 100

    val a = 1
    val b = 1
    val c = 5

    val alg = emma.parallelize {
      for (x <- DataBag(1 to N); y <- DataBag(1 to Math.sqrt(N).toInt); if a * x * x == b * y) yield (x, y, c)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch()
    val exp = alg.run(rt).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testTwoWayJoinComplexType(): Unit = {
    // Q: how many cannes winners are there in the IMDB top 100?
    val alg = emma.parallelize {
      val imdb = read(materializeResource("/cinema/imdb.csv"), new CSVInputFormat[IMDBEntry])
      val cannes = read(materializeResource("/cinema/canneswinners.csv"), new CSVInputFormat[FilmFestivalWinner])
      val berlin = read(materializeResource("/cinema/berlinalewinners.csv"), new CSVInputFormat[FilmFestivalWinner])

      val cwinners = for (x <- imdb; y <- cannes; if (x.title, x.year) ==(y.title, y.year)) yield ("Cannes", x.year, y.title)
      val bwinners = for (x <- imdb; y <- berlin; if (x.title, x.year) ==(y.title, y.year)) yield ("Berlin", x.year, y.title)

      (bwinners, cwinners)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native)
    val exp = alg.run(rt)

    // assert that the result contains the expected values
    compareBags(act._1.fetch(), exp._1.fetch())
    compareBags(act._2.fetch(), exp._2.fetch())
  }

  @Test def testMultiWayJoinSimpleType(): Unit = {
    val N = 10000

    val alg = emma.parallelize {
      for (x <- DataBag(1 to Math.sqrt(N).toInt);
           y <- DataBag(1 to Math.sqrt(N).toInt);
           z <- DataBag(1 to N); if x * x + y * y == z * z) yield (x, y, z)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch()
    val exp = alg.run(rt).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testMultiWayJoinComplexType(): Unit = {
    // Q: how many Cannes or Berlinale winners are there in the IMDB top 100?
    val alg = emma.parallelize {
      val imdbTop100 = read(materializeResource("/cinema/imdb.csv"), new CSVInputFormat[IMDBEntry])
      val cannesWinners = read(materializeResource("/cinema/canneswinners.csv"), new CSVInputFormat[FilmFestivalWinner])
      val berlinWinners = read(materializeResource("/cinema/berlinalewinners.csv"), new CSVInputFormat[FilmFestivalWinner])

      val cannesTop100 = for (w <- cannesWinners; m <- imdbTop100; if (w.title, w.year) ==(m.title, m.year)) yield ("Berlin", m.year, w.title)
      val berlinTop100 = for (w <- berlinWinners; m <- imdbTop100; if (w.title, w.year) ==(m.title, m.year)) yield ("Cannes", m.year, w.title)

      val result = cannesTop100 plus berlinTop100
      result
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch()
    val exp = alg.run(rt).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  // --------------------------------------------------------------------------
  // FoldGroup (Aggregations)
  // --------------------------------------------------------------------------

  @Test def testBasicGroup() = {
    val alg = emma.parallelize {
      val imdbTop100 = read(materializeResource("/cinema/imdb.csv"), new CSVInputFormat[IMDBEntry])
      for (g <- imdbTop100.groupBy(_.year)) yield g.values.count()
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch()
    val exp = alg.run(rt).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testSimpleGroup() = {
    val alg = emma.parallelize {
      val imdbTop100 = read(materializeResource("/cinema/imdb.csv"), new CSVInputFormat[IMDBEntry])
      for (g <- imdbTop100.groupBy(_.year / 10)) yield (s"${g.key * 10} - ${g.key * 10 + 9}", g.values.count())
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch().sortBy(_._1)
    val exp = alg.run(rt).fetch().sortBy(_._1)

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testComplexGroup() = {
    val alg = emma.parallelize {
      val imdbTop100 = read(materializeResource("/cinema/imdb.csv"), new CSVInputFormat[IMDBEntry])
      for (g <- imdbTop100.groupBy(_.year / 10)) yield {
        val total = g.values.count()
        val avgRating = g.values.map(_.rating.toInt * 10).sum() / (total * 10.0)
        val minRating = g.values.map(_.rating).min()
        val maxRating = g.values.map(_.rating).max()

        (s"${g.key * 10} - ${g.key * 10 + 9}", total, avgRating, minRating, maxRating)
      }
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(runtime.Native).fetch().sortBy(_._1)
    val exp = alg.run(rt).fetch().sortBy(_._1)

    // assert that the result contains the expected values
    compareBags(act, exp)
  }
}