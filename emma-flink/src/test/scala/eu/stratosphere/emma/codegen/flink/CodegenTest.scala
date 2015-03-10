package eu.stratosphere.emma.codegen.flink

import java.io.File

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.codegen.flink.testschema._
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.runtime.{Flink, FlinkLocal}
import eu.stratosphere.emma.testutil._
import org.junit.{After, Before, Test}

import scala.reflect.runtime.universe._

class CodegenTest {

  var rt: Flink = _

  var inBase = tempPath("test/input")
  var outBase = tempPath("test/output")

  @Before def setup() {
    // create a new runtime session
    rt = FlinkLocal("localhost", 6123)
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
      EdgeWithLabel(1L, 4L, "A"),
      EdgeWithLabel(2L, 5L, "B"),
      EdgeWithLabel(3L, 6L, "C")))
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

  @Test def testCSVReadWriteCaseClassType(): Unit = {
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
    val actPath = alg("native").run(runtime.Native())
    val resPath = alg("flink").run(rt)

    val exp = scala.io.Source.fromFile(resPath).getLines().toStream
    val res = scala.io.Source.fromFile(actPath).getLines().toStream

    // assert that the result contains the expected values
    compareBags(exp, res)
  }

  // --------------------------------------------------------------------------
  // Filter
  // --------------------------------------------------------------------------

  @Test def testFilterSimpleType(): Unit = {
    val inp = scala.io.Source.fromFile(materializeResource("/lyrics/Jabberwocky.txt")).getLines().toStream

    val len = 10

    val alg = emma.parallelize {
      DataBag(inp).withFilter(_.length > len)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testFilterTupleType(): Unit = {
    val inp = scala.io.Source.fromFile(materializeResource("/lyrics/Jabberwocky.txt")).getLines().zipWithIndex.toStream

    val len = 10

    val alg = emma.parallelize {
      DataBag(inp).withFilter(_._1.length > len)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testFilterCaseClassType(): Unit = {
    val inp = read(materializeResource("/cinema/canneswinners.csv"), new CSVInputFormat[FilmFestivalWinner]).fetch()

    val len = 10
    val year = 1980

    val alg = emma.parallelize {
      DataBag(inp).withFilter(_.year > year).withFilter(_.title.length > len)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
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
      for (x <- DataBag(inp)) yield if (x < 5) offset else offset + x / denominator
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testMapTupleType(): Unit = {
    val inp = Seq(
      (1L, 4L, "A"),
      (2L, 5L, "B"),
      (3L, 6L, "C"))

    val a = 1
    val b = 10

    val alg = emma.parallelize {
      for (e <- DataBag(inp)) yield if (e._1 < e._2) (e._1 * a, e._2 * b, a * b) else (e._2 * a, e._1 * b, a * b)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testMapCaseClassType(): Unit = {
    val inp = Seq(
      EdgeWithLabel(1L, 4L, "A"),
      EdgeWithLabel(2L, 5L, "B"),
      EdgeWithLabel(3L, 6L, "C"))

    val y = "Y"

    val alg = emma.parallelize {
      // FIXME: for some reason, does not work on companion apply constructor 'EdgeWithLabel(e.src, e.dst, y)'
      for (e <- DataBag(inp)) yield if (e.label == "B") new EdgeWithLabel(e.src, e.dst, y) else e.copy(label = y)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  // --------------------------------------------------------------------------
  // FlatMap
  // --------------------------------------------------------------------------

  @Test def testFlatMap(): Unit = {
    val inp = scala.io.Source.fromFile(materializeResource("/lyrics/Jabberwocky.txt")).getLines().toStream

    val max = 3
    val min = 9

    val alg = emma.parallelize {
      DataBag(inp).flatMap(x => DataBag(x.split("\\W+").filter(w => w.length > min && w.length < max)))
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testFlatMapWithFilter(): Unit = {
    val inp = scala.io.Source.fromFile(materializeResource("/lyrics/Jabberwocky.txt")).getLines().toStream

    val max = 3
    val min = 9
    val len = 10

    val alg = emma.parallelize {
      DataBag(inp).flatMap(x => DataBag(x.split("\\W+").filter(w => w.length > min && w.length < max))).withFilter(_.length > len)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

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
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testDistinctTupleType(): Unit = {
    val inp = {
      val lines = scala.io.Source.fromFile(materializeResource("/lyrics/Jabberwocky.txt")).getLines()
      lines.flatMap(_.split("\\W+")).zipWithIndex.toStream
    }

    val alg = emma.parallelize {
      DataBag(inp).distinct()
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testUnionSimpleType(): Unit = {
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
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  // --------------------------------------------------------------------------
  // Join & Cross
  // --------------------------------------------------------------------------

  @Test def testTwoWayCrossSimpleType(): Unit = {
    val N = 100

    val a = 1
    val b = 2

    val alg = emma.parallelize {
      for (x <- DataBag(1 to N); y <- DataBag(1 to Math.sqrt(N).toInt)) yield (a * x, b * y, a * b)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testTwoWayJoinSimpleType(): Unit = {
    val N = 100

    val a = 1
    val b = 2

    val alg = emma.parallelize {
      val A = DataBag(1 to N)
      val B = DataBag(1 to N)

      for (x <- A; y <- B; if x * a == y * b) yield (a * x, b * y, a * b)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testTwoWayJoinTupleType(): Unit = {
    // Q: how many cannes winners are there in the IMDB top 100?
    val alg = emma.parallelize {
      val A = DataBag((1 to 100).zipWithIndex.toSeq)
      val B = DataBag((1 to 100).zipWithIndex.toSeq)

      for (a <- A; b <- B; if a._1 == b._1) yield (a, b)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt)
    val exp = alg.run(runtime.Native())

    // assert that the result contains the expected values
    compareBags(act.fetch(), exp.fetch())
  }

  // FIXME: fails with an error: cannot determine unique overloaded method alternative from
  //   final private val title: java.lang.String
  //   def title(): java.lang.String
  // that matches value title:=> String
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
    val (cact, bact) = alg.run(runtime.Native())
    val (cexp, bexp) = alg.run(rt)

    // assert that the result contains the expected values
    compareBags(cact.fetch(), cexp.fetch())
    compareBags(bact.fetch(), bexp.fetch())
  }

  @Test def testMultiWayJoinSimpleType(): Unit = {
    val N = 10000

    val alg = emma.parallelize {
      for (x <- DataBag(1 to Math.sqrt(N).toInt);
           y <- DataBag(1 to Math.sqrt(N).toInt);
           z <- DataBag(1 to N); if x * x + y * y == z * z) yield (x, y, z)
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testMultiWayJoinComplexTypeLocalInput(): Unit = {
    val imdbTop100Local = read(materializeResource("/cinema/imdb.csv"), new CSVInputFormat[IMDBEntry]).fetch()
    val cannesWinnersLocal = read(materializeResource("/cinema/canneswinners.csv"), new CSVInputFormat[FilmFestivalWinner]).fetch()
    val berlinWinnersLocal = read(materializeResource("/cinema/berlinalewinners.csv"), new CSVInputFormat[FilmFestivalWinner]).fetch()

    // Q: how many Cannes or Berlinale winners are there in the IMDB top 100?
    val alg = emma.parallelize {
      val imdbTop100 = DataBag(imdbTop100Local)
      val cannesWinners = DataBag(cannesWinnersLocal)
      val berlinWinners = DataBag(berlinWinnersLocal)

      val cannesTop100 = for (w <- cannesWinners; m <- imdbTop100; if (w.title, w.year) ==(m.title, m.year)) yield (m.year, w.title)
      val berlinTop100 = for (w <- berlinWinners; m <- imdbTop100; if (w.title, w.year) ==(m.title, m.year)) yield (m.year, w.title)

      val result = cannesTop100 plus berlinTop100
      result
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testMultiWayJoinComplexType(): Unit = {
    // Q: how many Cannes or Berlinale winners are there in the IMDB top 100?
    val alg = emma.parallelize {
      val imdbTop100 = read(materializeResource("/cinema/imdb.csv"), new CSVInputFormat[IMDBEntry])
      val cannesWinners = read(materializeResource("/cinema/canneswinners.csv"), new CSVInputFormat[FilmFestivalWinner])
      val berlinWinners = read(materializeResource("/cinema/berlinalewinners.csv"), new CSVInputFormat[FilmFestivalWinner])

      val cannesTop100 = for (w <- cannesWinners; m <- imdbTop100; if (w.title, w.year) ==(m.title, m.year)) yield (m.year, w.title)
      val berlinTop100 = for (w <- berlinWinners; m <- imdbTop100; if (w.title, w.year) ==(m.title, m.year)) yield (m.year, w.title)

      val result = cannesTop100 plus berlinTop100
      result
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  // --------------------------------------------------------------------------
  // FoldGroup (Aggregations)
  // --------------------------------------------------------------------------

  @Test def testGroupSimpleType() = {
    val N = 200

    val alg = emma.parallelize {
      for (x <- DataBag(1 to N map (x => (0, x))).groupBy(_._1)) yield x.values.map(_._2).sum()
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testBasicGroup() = {
    val alg = emma.parallelize {
      val imdbTop100 = read(materializeResource("/cinema/imdb.csv"), new CSVInputFormat[IMDBEntry])
      for (g <- imdbTop100.groupBy(_.year)) yield g.values.count()
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch()
    val exp = alg.run(runtime.Native()).fetch()

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
    val act = alg.run(rt).fetch().sortBy(_._1)
    val exp = alg.run(runtime.Native()).fetch().sortBy(_._1)

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  @Test def testGroupWithComplexKey() = {
    val alg = emma.parallelize {
      val imdbTop100 = read(materializeResource("/cinema/imdb.csv"), new CSVInputFormat[IMDBEntry])
      for (g <- imdbTop100.groupBy(x => (x.year / 10, x.rating.toInt))) yield (g.key._1, g.key._2, g.values.count())
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt).fetch().sortBy(_._1)
    val exp = alg.run(runtime.Native()).fetch().sortBy(_._1)

    // assert that the result contains the expected values
    compareBags(act, exp)
  }

  // --------------------------------------------------------------------------
  // Fold (Global Aggregations)
  // --------------------------------------------------------------------------

  @Test def testFoldSimpleType() = {
    val N = 200

    val alg = emma.parallelize {
      DataBag(1 to N).sum()
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt)
    val exp = alg.run(runtime.Native())

    // assert that the result contains the expected values
    assert(exp == act, s"Unexpected result: $act != $exp")
  }

  @Test def testFoldComplexType() = {
    val alg = emma.parallelize {
      val imdbTop100 = read(materializeResource("/cinema/imdb.csv"), new CSVInputFormat[IMDBEntry])
      imdbTop100.count()
    }

    // compute the algorithm using the original code and the runtime under test
    val act = alg.run(rt)
    val exp = alg.run(runtime.Native())

    // assert that the result contains the expected values
    assert(exp == act, s"Unexpected result: $act != $exp")
  }
}