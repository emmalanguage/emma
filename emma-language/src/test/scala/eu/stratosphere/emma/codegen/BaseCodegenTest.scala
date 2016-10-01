/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.stratosphere.emma.codegen

import java.io.File

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.testutil._

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.reflect.runtime.universe._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
abstract class BaseCodegenTest(rtName: String)
  extends FreeSpec  with Matchers with BeforeAndAfter {

  val inputDir = tempPath("test/input")
  val outputDir = tempPath("test/output")
  def runtimeUnderTest: runtime.Engine

  object from {
    lazy val jabberwocky = fromPath(materializeResource("/lyrics/Jabberwocky.txt"))

    lazy val imdb = read(materializeResource("/cinema/imdb.csv"),
      new CSVInputFormat[ImdbMovie]).fetch()

    lazy val cannes = read(materializeResource("/cinema/canneswinners.csv"),
      new CSVInputFormat[FilmFestWinner]).fetch()

    lazy val berlin = read(materializeResource("/cinema/berlinalewinners.csv"),
      new CSVInputFormat[FilmFestWinner]).fetch()
  }
  
  before {
    // make sure that the base paths exist
    new File(inputDir).mkdirs()
    new File(outputDir).mkdirs()
  }

  after {
    deleteRecursive { new File(outputDir) }
    deleteRecursive { new File(inputDir) }
  }

  // --------------------------------------------------------------------------
  // CSV converters
  // --------------------------------------------------------------------------

  "CSV converters" - {
    "read/write case classes" in
      testCsvRw[LabelledEdge[Long, String]] { Seq(
        LabelledEdge(1, 4, "A"),
        LabelledEdge(2, 5, "B"),
        LabelledEdge(3, 6, "C"))
      }

    def testCsvRw[A : TypeTag : CSVConverters](input: Seq[A]) =
      withRuntime(runtimeUnderTest) { engine =>
        withRuntime(runtime.Native()) { native =>
          // construct a parameterized algorithm family
          val algorithm = (suffix: String) => emma.parallelize {
            val path = s"$outputDir/csv.$suffix"
            // write out the original input
            write(path, new CSVOutputFormat[A]) { DataBag(input) }
            // return the output path
            path
          }

          // write the input to a file using the original code and the runtime under test
          val actual = fromPath(algorithm(rtName).run(engine))
          val expected = fromPath(algorithm("native").run(native))
          // assert that the result contains the expected values
          actual should be (expected)
        }
      }
    }

  // --------------------------------------------------------------------------
  // Filter
  // --------------------------------------------------------------------------

  "Filter" - {
    "strings" in emma.parallelize {
      DataBag(from.jabberwocky) withFilter { _.length > 10 }
    }.verifyWith(runtimeUnderTest)

    "tuples" in emma.parallelize {
      DataBag(from.jabberwocky.zipWithIndex) withFilter { _._1.length > 10 }
    }.verifyWith(runtimeUnderTest)

    "case classes" in emma.parallelize {
      DataBag(from.imdb)
        .withFilter { _.year > 1980 }
        .withFilter { _.title.length > 10 }
    }.verifyWith(runtimeUnderTest)
  }

  // --------------------------------------------------------------------------
  // Map
  // --------------------------------------------------------------------------

  "Map" - {
    "primitives" in emma.parallelize {
      val whiteList = DataBag(1 to 5)
      for { even <- DataBag(2 to 10 by 2) }
        yield if (whiteList exists { _ == even }) even else 0
    }.verifyWith(runtimeUnderTest)

    "tuples" in emma.parallelize {
      for { edge <- DataBag((1, 4, "A") :: (2, 5, "B") :: (3, 6, "C") :: Nil) }
        yield if (edge._1 < edge._2) edge._1 -> edge._2 else edge._2 -> edge._1
    }.verifyWith(runtimeUnderTest)

    "case classes" in {
      val graph = Seq(
        LabelledEdge(1, 4, "A"),
        LabelledEdge(2, 5, "B"),
        LabelledEdge(3, 6, "C"))

      emma.parallelize {
        for { edge <- DataBag(graph) } yield
          if (edge.label == "B") LabelledEdge(edge.dst, edge.src, "B")
          else edge.copy(label = "Y")
      }.verifyWith(runtimeUnderTest)
    }
  }

  // --------------------------------------------------------------------------
  // FlatMap
  // --------------------------------------------------------------------------

  "FlatMap" - {
    "strings" in emma.parallelize {
      DataBag(from.jabberwocky) flatMap { line =>
        DataBag(line split "\\W+" filter { word =>
          word.length > 3 && word.length < 9
        })
      }
    }.verifyWith(runtimeUnderTest)

    "with filter" in emma.parallelize {
      DataBag(from.jabberwocky) flatMap { line =>
        DataBag(line split "\\W+" filter {
          word => word.length > 3 && word.length < 9
        })
      } withFilter { _.length > 5 }
    }.verifyWith(runtimeUnderTest)

    "comprehension with uncorrelated result" in emma.parallelize {
      for {
        line <- DataBag(from.jabberwocky)
        word <- DataBag(line split "\\W+" filter { word =>
          word.length > 3 && word.length < 9
        }) if word.length > 5
      } yield word
    }.verifyWith(runtimeUnderTest)

    "comprehension with correlated result" in emma.parallelize {
      for {
        line <- DataBag(from.jabberwocky)
        word <- DataBag(line split "\\W+")
      } yield (line, word)
    }.verifyWith(runtimeUnderTest)
  }

  // --------------------------------------------------------------------------
  // Distinct and Union
  // --------------------------------------------------------------------------

  "Distinct" - {
    "strings" in emma.parallelize {
      DataBag(from.jabberwocky flatMap { _ split "\\W+" }).distinct()
    }.verifyWith(runtimeUnderTest)

    "tuples" in emma.parallelize {
      DataBag(from.jabberwocky.flatMap { _ split "\\W+" }.zipWithIndex).distinct()
    }.verifyWith(runtimeUnderTest)
  }

  "Union" in {
    val (even, odd) = from.jabberwocky
      .flatMap { _ split "\\W+" }
      .partition { _.length % 2 == 0 }

    emma.parallelize {
      DataBag(even) plus DataBag(odd)
    }.verifyWith(runtimeUnderTest)
  }

  // --------------------------------------------------------------------------
  // Join & Cross
  // --------------------------------------------------------------------------

  "Join" - {
    "two-way on primitives" in emma.parallelize {
      for {
        x <- DataBag(1 to 50)
        y <- DataBag(1 to 100)
        if x == 2 * y
      } yield (x, 2 * y, 2)
    }.verifyWith(runtimeUnderTest)

    "two-way on tuples" in emma.parallelize {
      for {
        x <- DataBag(5.to(15).zipWithIndex)
        y <- DataBag(1.to(20).zipWithIndex)
        if x._1 == y._1
      } yield (x, y)
    }.verifyWith(runtimeUnderTest)

    // Q: how many cannes winners are there in the IMDB top 100?
    "two-way on case classes" in emma.parallelize {
      val cannesTop100 = for {
        movie <- DataBag(from.imdb)
        winner <- DataBag(from.cannes)
        if (movie.title, movie.year) == (winner.title, winner.year)
      } yield ("Cannes", movie.year, winner.title)

      val berlinTop100 = for {
        movie <- DataBag(from.imdb)
        winner <- DataBag(from.berlin)
        if (movie.title, movie.year) == (winner.title, winner.year)
      } yield ("Berlin", movie.year, winner.title)

      berlinTop100 plus cannesTop100
    }.verifyWith(runtimeUnderTest)

    "multi-way on primitives" in emma.parallelize {
      for {
        x <- DataBag(1 to 10)
        y <- DataBag(1 to 20)
        z <- DataBag(1 to 100)
        if x * x + y * y == z * z
      } yield (x, y, z)
    }.verifyWith(runtimeUnderTest)

    "multi-way on case classes with local input" in {
      val imdbMovies = from.imdb
      val cannesWinners = from.cannes
      val berlinWinnera = from.berlin

      // Q: how many Cannes or Berlinale winners are there in the IMDB top 100?
      emma.parallelize {
        val cannesTop100 = for {
          movie <- DataBag(imdbMovies)
          winner <- DataBag(cannesWinners)
          if (winner.title, winner.year) == (movie.title, movie.year)
        } yield (movie.year, winner.title)

        val berlinTop100 = for {
          movie <- DataBag(imdbMovies)
          winner <- DataBag(berlinWinnera)
          if (winner.title, winner.year) == (movie.title, movie.year)
        } yield (movie.year, winner.title)

        cannesTop100 plus berlinTop100
      }.verifyWith(runtimeUnderTest)
    }
  }

  "Cross" in emma.parallelize {
    for {
      x <- DataBag(3 to 100 by 3)
      y <- DataBag(5 to 100 by 5)
    } yield x * y
  }.verifyWith(runtimeUnderTest)

  // --------------------------------------------------------------------------
  // Group (with materialization) and FoldGroup (aggregations)
  // --------------------------------------------------------------------------

  "Group" - {
    "materialization" in emma.parallelize {
      DataBag(Seq(1)) groupBy identity
    }.verifyWith(runtimeUnderTest)

    "materialization with closure" in emma.parallelize {
      val semiFinal = 8
      val bag = DataBag(new Random shuffle 0.until(100).toList)
      val top = for (g <- bag groupBy { _ % semiFinal })
        yield g.values.fetch().sorted.take(semiFinal / 2).sum

      top.max
    }.verifyWith(runtimeUnderTest)
  }

  "FoldGroup" - {
    "of primitives" in emma.parallelize {
      for (g <- DataBag(1 to 100 map { _ -> 0 }) groupBy { _._1 })
        yield g.values.map { _._2 }.sum
    }.verifyWith(runtimeUnderTest)

    "of case classes" in emma.parallelize {
      for (yearly <- DataBag(from.imdb) groupBy { _.year })
        yield yearly.values.size
    }.verifyWith(runtimeUnderTest)

    "of case classes multiple times" in emma.parallelize {
      val movies = DataBag(from.imdb)
      
      for (decade <- movies groupBy { _.year / 10 }) yield {
        val total = decade.values.size
        val avgRating = decade.values.map { _.rating.toInt * 10 }.sum / (total * 10.0)
        val minRating = decade.values.map { _.rating }.min
        val maxRating = decade.values.map { _.rating }.max

        (s"${decade.key * 10} - ${decade.key * 10 + 9}",
          total, avgRating, minRating, maxRating)
      }
    }.verifyWith(runtimeUnderTest)

    "with a complex key" in emma.parallelize {
      val yearlyRatings = DataBag(from.imdb)
        .groupBy { movie => (movie.year / 10, movie.rating.toInt) }
      
      for (yr <- yearlyRatings) yield {
        val (year, rating) = yr.key
        (year, rating, yr.values.size)
      }
    }.verifyWith(runtimeUnderTest)

    "with duplicate group names" in emma.parallelize {
      val movies = DataBag(from.imdb)

      val leastPopular = for {
        decade <- movies groupBy { _.year / 10 }
      } yield (decade.key, decade.values.size, decade.values.map { _.rating }.min)

      val mostPopular = for {
        decade <- movies groupBy { _.year / 10 }
      } yield (decade.key, decade.values.size, decade.values.map { _.rating }.max)

      (leastPopular, mostPopular)
    }.verifyWith(runtimeUnderTest)

    "with multiple groups in the same comprehension" in emma.parallelize {
      for {
        can10 <- DataBag(from.cannes) groupBy { _.year / 10 }
        ber10 <- DataBag(from.berlin) groupBy { _.year / 10 }
        if can10.key == ber10.key
      } yield (can10.values.size, ber10.values.size)
    }.verifyWith(runtimeUnderTest)
  }

  // --------------------------------------------------------------------------
  // Fold (global aggregations)
  // --------------------------------------------------------------------------

  "Fold" - {
    "of an empty DataBag (nonEmpty)" in emma.parallelize {
      (DataBag().nonEmpty, DataBag(1 to 3).nonEmpty)
    }.verifyWith(runtimeUnderTest)

    "of primitives (fold)" in emma.parallelize {
      DataBag(0 until 100).fold(0)(identity, _ + _)
    }.verifyWith(runtimeUnderTest)

    "of primitives (sum)" in emma.parallelize {
      DataBag(1 to 200).sum
    }.verifyWith(runtimeUnderTest)

    "of case classes (count)" in emma.parallelize {
      DataBag(from.imdb).size
    }.verifyWith(runtimeUnderTest)
  }

  // --------------------------------------------------------------------------
  // Stateful DataBags
  // --------------------------------------------------------------------------

  "Stateful" - {
    "create (fetch)" in withRuntime(runtimeUnderTest) { engine =>
      val input = State(6, 8) :: Nil

      val algorithm = emma.parallelize {
        val withState = stateful[State, Long] { DataBag(input) }
        withState.bag().fetch()
      }

      algorithm.run(engine) should equal (input)
    }

    "update" in { // FIXME: doesn't work if we move `input` inside `parallelize`
      val input = State(6, 12) :: State(3, 4) :: Nil
      emma.parallelize {
        val withState = stateful[State, Long] { DataBag(input) }

        withState.updateWithZero { s =>
          s.value += 1
          DataBag(): DataBag[Int]
        }

        val updates1 = DataBag(Update(6, 5) :: Update(6, 7) :: Nil)
        withState.updateWithOne(updates1)(_.identity, (s, u) => {
          s.value += u.inc
          DataBag(42 :: Nil)
        })

        val updates2 = DataBag(Update(3, 5) :: Update(3, 4) :: Nil)
        withState.updateWithMany(updates2)(_.identity, (s, us) => {
          for (u <- us) yield s.value += u.inc
        })

        val updates3 = DataBag(Update(3, 1) :: Update(6, 2) :: Update(6, 3) :: Nil)
        withState.updateWithMany(updates3)(_.identity, (s, us) => {
          us.map { _.inc }.sum
          DataBag(42 :: Nil)
        })

        withState.bag()
      }.verifyWith(runtimeUnderTest)
    }
  }

  // --------------------------------------------------------------------------
  // Expression normalization
  // --------------------------------------------------------------------------

  "Normalization" - {
    "of filters with simple predicates" in emma.parallelize {
      for {
        x <- DataBag(1 to 1000)
        if !(x > 5 || (x < 2 && x == 0)) || (x > 5 || !(x < 2))
      } yield x
    }.verifyWith(runtimeUnderTest)

    "of filters with simple predicates and multiple inputs" in emma.parallelize {
      for {
        x <- DataBag(1 to 1000)
        y <- DataBag(100 to 2000)
        if x < y || x + y < 100 && x % 2 == 0 || y / 2 == 0
      } yield y + x
    }.verifyWith(runtimeUnderTest)

    "of filters with UDF predicates" in emma.parallelize {
      for {
        x <- DataBag(1 to 1000)
        if !(p1(x) || (p2(x) && p3(x))) || (p1(x) || !p2(x))
      }yield x
    }.verifyWith(runtimeUnderTest)

    // otherwise ImdbYear could not be found
    "of case class names" in  emma.parallelize {
      val movies = DataBag(from.imdb)
      val years = for (mov <- movies) yield ImdbYear(mov.year)
      years forall { case iy @ ImdbYear(y) => iy == new ImdbYear(y) }
    }.verifyWith(runtimeUnderTest)

    "of enclosing class parameters" in {
      // a class that wraps an Emma program and a parameter used within the `parallelize` call
      case class MoviesWithinPeriodQuery(minYear: Int, period: Int) {
        lazy val algorithm = emma.parallelize {
          for {
            movie <- DataBag(from.imdb)
            if movie.year >= minYear && movie.year < minYear + period
          } yield movie
        }
      }

      // run the algorithm
      MoviesWithinPeriodQuery(1990, 10)
        .algorithm.verifyWith(runtimeUnderTest)
    }

    "of local functions" in emma.parallelize {
      val double = (x: Int) => 2 * x
      val add = (x: Int, y: Int) => x + y

      val times2 = for { x <- DataBag(1 to 100) } yield double(x)
      val increment5 = for { x <- DataBag(1 to 100) } yield add(x, 5)

      times2 plus increment5
    }.verifyWith(runtimeUnderTest)
  }

  // --------------------------------------------------------------------------
  // Miscellaneous
  // --------------------------------------------------------------------------

  "Miscellaneous" - {
    "Pattern matching in `yield`" in emma.parallelize {
      val range = DataBag(0.to(100).zipWithIndex)
      val squares = for (xy <- range) yield xy match { case (x, y) => x + y }
      squares.sum
    }.verifyWith(runtimeUnderTest)

    "Map with partial function" in emma.parallelize {
      val range = DataBag(0.to(100).zipWithIndex)
      val squares = range map { case (x, y) => x + y }
      squares.sum
    }.verifyWith(runtimeUnderTest)

    "Destructuring of a generator" in emma.parallelize {
      val range = DataBag(0.to(100).zipWithIndex)
      val squares = for { (x, y) <- range } yield x + y
      squares.sum
    }.verifyWith(runtimeUnderTest)

    "Intermediate value definition" in emma.parallelize {
      val range = DataBag(0.to(100).zipWithIndex)
      val squares = for (xy <- range; sqr = xy._1 * xy._2) yield sqr
      squares.sum
    }.verifyWith(runtimeUnderTest)

    "Root package capture" in emma.parallelize {
      val eu = "eu"
      val com = "com"
      val java = "java"
      val org = "org"
      val scala = "scala"
      DataBag(0 to 100).sum
    }.verifyWith(runtimeUnderTest)

    "Constant expressions" in emma.parallelize {
      val as = for { x <- DataBag(1 to 100) } yield 1 // map
      val bs = DataBag(101 to 200) flatMap { _ => DataBag(2 to 4) } // flatMap
      val cs = for { x <- DataBag(201 to 300) if 5 == 1 } yield 5 // filter
      val ds = DataBag(301 to 400) withFilter { _ => true } // filter
      as plus bs plus cs plus ds
    }.verifyWith(runtimeUnderTest)

    "Updated tmp sink (sieve of Eratosthenes)" in emma.parallelize {
      val N = 20
      val payload = "#" * 100

      val positive = {
        var primes = DataBag(3 to N map { (_, payload) })
        var p = 2

        while (p <= math.sqrt(N)) {
          primes = for { (n, payload) <- primes if n > p && n % p != 0 } yield (n, payload)
          p = primes.map { _._1 }.min
        }

        primes map { _._1 }
      }

      val negative = {
        var primes = DataBag(-N to 3 map { (_, payload) })
        var p = -2

        while (p >= -math.sqrt(N)) {
          primes = for { (n, payload) <- primes if n < p && n % p != 0 } yield (n, payload)
          p = primes.map { _._1 }.max
        }

        primes map { _._1 }
      }

      positive plus negative
    }.verifyWith(runtimeUnderTest)

    "val destructuring" in emma.parallelize {
      val resource = materializeResource("/cinema/imdb.csv")
      val imdbTop100 = read(resource, new CSVInputFormat[ImdbMovie])
      val ratingsPerDecade = for {
        group <- imdbTop100.groupBy(mov => (mov.year / 10, mov.rating.round))
      } yield {
        val (year, rating) = group.key
        (year, rating, group.values.size)
      }

      for {
        r <- ratingsPerDecade
        m <- imdbTop100
        if r == (m.year, m.rating.round, 1L)
      } yield (r, m)
    }.verifyWith(runtimeUnderTest)
  }
}
