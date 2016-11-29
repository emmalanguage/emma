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
package org.emmalanguage
package compiler

import api._
import io.csv._
import test.schema.Graphs._
import test.schema.Movies._
import test.schema.Math._
import test.util._

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom
import scala.util.Random

import java.io.File
import java.nio.file.Paths

@RunWith(classOf[JUnitRunner])
abstract class BaseCodegenSpec
  extends BaseCompilerSpec with BeforeAndAfter {

  import BaseCodegenSpec._

  import compiler._

  val inputDir = tempPath("test/input")
  val outputDir = tempPath("test/output")
  implicit val imdbMovieCSVConverter = CSVConverter[ImdbMovie]

  def backendPipeline: u.Tree => u.Tree

  val codegenPipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true)(
      checkValid,
      Core.lift,
      Comprehension.combine,
      backendPipeline
    ).compose(_.tree)

  def verify(e: u.Expr[Any]): Unit = {
    val expected = eval[Any](idPipeline(e))
    val actual = eval[Any](codegenPipeline(e))
    assert(actual == expected, s"""
                                  |actual != expected
                                  |actual: ${show(actual)}
                                  |expected: ${show(expected)}
                                  |""".stripMargin)
  }

  lazy val checkValid = (tree: u.Tree) => if (!Source.validate(tree)) fail else tree

  def show(x: Any): String = x match {
    case _: DataBag[_] => x.asInstanceOf[DataBag[_]].fetch().toString
    case _ => x.toString
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
  // Filter
  // --------------------------------------------------------------------------

  "Filter" - {
    "strings" in verify(u.reify {
      DataBag(jabberwocky) withFilter { _.length > 10 }
    })

    "tuples" in verify(u.reify {
      DataBag(jabberwocky map {(_,1)}) withFilter { _._1.length > 10 }
    })

    "case classes" in verify(u.reify {
      DataBag(imdb)
        .withFilter { _.year > 1980 }
        .withFilter { _.title.length > 10 }
    })
  }

  // --------------------------------------------------------------------------
  // Map
  // --------------------------------------------------------------------------

  "Map" - {
    "primitives" in verify(u.reify {
      val whiteList = DataBag(1 to 5)
      for { even <- DataBag(2 to 10 by 2) }
        yield if (whiteList exists { _ == even }) even else 0
    })

    "tuples" in verify(u.reify {
      for { edge <- DataBag((1, 4, "A") :: (2, 5, "B") :: (3, 6, "C") :: Nil) }
        yield if (edge._1 < edge._2) edge._1 -> edge._2 else edge._2 -> edge._1
    })

    "case classes" in {
      verify(u.reify {
        for { edge <- DataBag(graph) } yield
          if (edge.label == "B") LabelledEdge(edge.dst, edge.src, "B")
          else edge.copy(label = "Y")
      })
    }
  }

  // --------------------------------------------------------------------------
  // FlatMap
  // --------------------------------------------------------------------------

  "FlatMap" - {
    "strings" in verify(u.reify {
      DataBag(jabberwocky) flatMap { line =>
        DataBag(line split "\\W+" filter { word =>
          word.length > 3 && word.length < 9
        })
      }
    })

    "with filter" in verify(u.reify {
      DataBag(jabberwocky) flatMap { line =>
        DataBag(line split "\\W+" filter {
          word => word.length > 3 && word.length < 9
        })
      } withFilter { _.length > 5 }
    })

    "comprehension with uncorrelated result" in verify(u.reify {
      for {
        line <- DataBag(jabberwocky)
        word <- DataBag(line split "\\W+" filter { word =>
          word.length > 3 && word.length < 9
        }) if word.length > 5
      } yield word
    })

    "comprehension with correlated result" in verify(u.reify {
      for {
        line <- DataBag(jabberwocky)
        word <- DataBag(line split "\\W+")
      } yield (line, word)
    })
  }

  // --------------------------------------------------------------------------
  // Distinct and Union
  // --------------------------------------------------------------------------

  "Distinct" - {
    "strings" in verify(u.reify {
      DataBag(jabberwocky flatMap { _ split "\\W+" }).distinct
    })

    "tuples" in verify(u.reify {
      DataBag(jabberwocky.flatMap { _ split "\\W+" } map {(_,1)}).distinct
    })
  }

  "Union" in {
    verify(u.reify {
      DataBag(jabberwockyEven) union DataBag(jabberwockyOdd)
    })
  }

  // --------------------------------------------------------------------------
  // Join & Cross
  // --------------------------------------------------------------------------

  "Join" - {
    "two-way on primitives" in verify(u.reify {
      for {
        x <- DataBag(1 to 50)
        y <- DataBag(1 to 100)
        if x == 2 * y
      } yield (x, 2 * y, 2)
    })

    "two-way on tuples" in verify(u.reify {
      for {
        x <- DataBag(zipWithIndex(5 to 15))
        y <- DataBag(zipWithIndex(1 to 20))
        if x._1 == y._1
      } yield (x, y)
    })

    // Q: how many cannes winners are there in the IMDB top 100?
    "two-way on case classes" in verify(u.reify {
      val cannesTop100 = for {
        movie <- DataBag(imdb)
        winner <- DataBag(cannes)
        if (movie.title, movie.year) == (winner.title, winner.year)
      } yield ("Cannes", movie.year, winner.title)

      val berlinTop100 = for {
        movie <- DataBag(imdb)
        winner <- DataBag(berlin)
        if (movie.title, movie.year) == (winner.title, winner.year)
      } yield ("Berlin", movie.year, winner.title)

      berlinTop100 union cannesTop100
    })

    "multi-way on primitives" in verify(u.reify {
      for {
        x <- DataBag(1 to 10)
        y <- DataBag(1 to 20)
        z <- DataBag(1 to 100)
        if x * x + y * y == z * z
      } yield (x, y, z)
    })

    //todo: After we have parallelize on newir, move this to a test where we test parallelize
    // (it doesn't work here because of the closure)
//    "multi-way on case classes with local input" in {
//      val imdbMovies = imdb
//      val cannesWinners = cannes
//      val berlinWinnera = berlin
//
//      // Q: how many Cannes or Berlinale winners are there in the IMDB top 100?
//      verify(u.reify {
//        val cannesTop100 = for {
//          movie <- DataBag(imdbMovies)
//          winner <- DataBag(cannesWinners)
//          if (winner.title, winner.year) == (movie.title, movie.year)
//        } yield (movie.year, winner.title)
//
//        val berlinTop100 = for {
//          movie <- DataBag(imdbMovies)
//          winner <- DataBag(berlinWinnera)
//          if (winner.title, winner.year) == (movie.title, movie.year)
//        } yield (movie.year, winner.title)
//
//        cannesTop100 plus berlinTop100
//      })
//    }
  }

  "Cross" in verify(u.reify {
    for {
      x <- DataBag(3 to 100 by 3)
      y <- DataBag(5 to 100 by 5)
    } yield x * y
  })

  // --------------------------------------------------------------------------
  // Group (with materialization) and FoldGroup (aggregations)
  // --------------------------------------------------------------------------

  "Group" - {
    "materialization" in verify(u.reify {
      DataBag(Seq(1)) groupBy Predef.identity
    })

    "materialization with closure" in verify(u.reify {
      val semiFinal = 8
      val bag = DataBag(new Random shuffle 0.until(100).toList)
      val top = for (g <- bag groupBy { _ % semiFinal })
        yield g.values.fetch().toSeq.sorted.take(semiFinal / 2).sum

      top.max
    })
  }

  "FoldGroup" - {
    "of primitives" in verify(u.reify {
      for (g <- DataBag(1 to 100 map { _ -> 0 }) groupBy { _._1 })
        yield g.values.map { _._2 }.sum
    })

    "of case classes" in verify(u.reify {
      for (yearly <- DataBag(imdb) groupBy { _.year })
        yield yearly.values.size
    })

    "of case classes multiple times" in verify(u.reify {
      val movies = DataBag(imdb)

      for (decade <- movies groupBy { _.year / 10 }) yield {
        val total = decade.values.size
        val avgRating = decade.values.map { _.rating.toInt * 10 }.sum / (total * 10.0)
        val minRating = decade.values.map { _.rating }.min
        val maxRating = decade.values.map { _.rating }.max

        (s"${decade.key * 10} - ${decade.key * 10 + 9}",
          total, avgRating, minRating, maxRating)
      }
    })

    "with a complex key" in verify(u.reify {
      val yearlyRatings = DataBag(imdb)
        .groupBy { movie => (movie.year / 10, movie.rating.toInt) }

      for (yr <- yearlyRatings) yield {
        val (year, rating) = yr.key
        (year, rating, yr.values.size)
      }
    })

    "with duplicate group names" in verify(u.reify {
      val movies = DataBag(imdb)

      val leastPopular = for {
        decade <- movies groupBy { _.year / 10 }
      } yield (decade.key, decade.values.size, decade.values.map { _.rating }.min)

      val mostPopular = for {
        decade <- movies groupBy { _.year / 10 }
      } yield (decade.key, decade.values.size, decade.values.map { _.rating }.max)

      (leastPopular, mostPopular)
    })

    "with multiple groups in the same comprehension" in verify(u.reify {
      for {
        can10 <- DataBag(cannes) groupBy { _.year / 10 }
        ber10 <- DataBag(berlin) groupBy { _.year / 10 }
        if can10.key == ber10.key
      } yield (can10.values.size, ber10.values.size)
    })
  }

  // --------------------------------------------------------------------------
  // Fold (global aggregations)
  // --------------------------------------------------------------------------

  "Fold" - {
    "of an empty DataBag (nonEmpty)" in verify(u.reify {
      //(DataBag[Int]().nonEmpty, DataBag(1 to 3).nonEmpty)
    })

    "of primitives (fold)" in verify(u.reify {
      DataBag(0 until 100).fold(0)(Predef.identity, _ + _)
    })

    "of primitives (sum)" in verify(u.reify {
      DataBag(1 to 200).sum
    })

    "of case classes (count)" in verify(u.reify {
      DataBag(imdb).size
    })
  }

// TODO: This is commented out, because we don't yet have stateful support on newir.
//  // --------------------------------------------------------------------------
//  // Stateful DataBags
//  // --------------------------------------------------------------------------
//
//  "Stateful" - {
//    "create (fetch)" in withRuntime(runtimeUnderTest) { engine =>
//      val input = State(6, 8) :: Nil
//
//      val algorithm = verify(u.reify {
//        val withState = stateful[State, Long] { DataBag(input) }
//        withState.bag().fetch()
//      })
//
//      algorithm.run(engine) should equal (input)
//    }
//
//    "update" in { // FIXME: doesn't work if we move `input` inside `parallelize`
//      val input = State(6, 12) :: State(3, 4) :: Nil
//      verify(u.reify {
//        val withState = stateful[State, Long] { DataBag(input) }
//
//        withState.updateWithZero { s =>
//          s.value += 1
//          DataBag(): DataBag[Int]
//        }
//
//        val updates1 = DataBag(Update(6, 5) :: Update(6, 7) :: Nil)
//        withState.updateWithOne(updates1)(_.identity, (s, u) => {
//          s.value += u.inc
//          DataBag(42 :: Nil)
//        })
//
//        val updates2 = DataBag(Update(3, 5) :: Update(3, 4) :: Nil)
//        withState.updateWithMany(updates2)(_.identity, (s, us) => {
//          for (u <- us) yield s.value += u.inc
//        })
//
//        val updates3 = DataBag(Update(3, 1) :: Update(6, 2) :: Update(6, 3) :: Nil)
//        withState.updateWithMany(updates3)(_.identity, (s, us) => {
//          us.map { _.inc }.sum
//          DataBag(42 :: Nil)
//        })
//
//        withState.bag()
//      })
//    }
//  }

  // --------------------------------------------------------------------------
  // Expression normalization
  // --------------------------------------------------------------------------

  "Normalization" - {
    "of filters with simple predicates" in verify(u.reify {
      for {
        x <- DataBag(1 to 1000)
        if !(x > 5 || (x < 2 && x == 0)) || (x > 5 || !(x < 2))
      } yield x
    })

    "of filters with simple predicates and multiple inputs" in verify(u.reify {
      for {
        x <- DataBag(1 to 1000)
        y <- DataBag(100 to 2000)
        if x < y || x + y < 100 && x % 2 == 0 || y / 2 == 0
      } yield y + x
    })

    "of filters with UDF predicates" in verify(u.reify {
      for {
        x <- DataBag(1 to 1000)
        if !(p1(x) || (p2(x) && p3(x))) || (p1(x) || !p2(x))
      } yield x
    })

    // otherwise ImdbYear could not be found
    "of case class names" in  verify(u.reify {
      val movies = DataBag(imdb)
      val years = for (mov <- movies) yield ImdbYear(mov.year)
      years forall { case iy @ ImdbYear(y) => iy == ImdbYear(y) }
    })

    //todo: After we have parallelize on newir, move this to a test where we test parallelize
    // (it doesn't work here because of the closure)
//    "of enclosing class parameters" in {
//      // a class that wraps an Emma program and a parameter used within the `parallelize` call
//      case class MoviesWithinPeriodQuery(minYear: Int, period: Int) {
//        lazy val algorithm = verify(u.reify {
//          for {
//            movie <- DataBag(imdb)
//            if movie.year >= minYear && movie.year < minYear + period
//          } yield movie
//        }
//      }
//
//      // run the algorithm
//      MoviesWithinPeriodQuery(1990, 10)
//        .algorithm)
//    }

    "of local functions" in verify(u.reify {
      val double = (x: Int) => 2 * x
      val add = (x: Int, y: Int) => x + y

      val times2 = for { x <- DataBag(1 to 100) } yield double(x)
      val increment5 = for { x <- DataBag(1 to 100) } yield add(x, 5)

      times2 union increment5
    })
  }

  // --------------------------------------------------------------------------
  // CSV IO
  // --------------------------------------------------------------------------

  "CSV" - {
    "read/write case classes" in verify(u.reify {
      val inputPath = materializeResource("/cinema/imdb.csv")
      val outputPath = Paths.get(s"${System.getProperty("java.io.tmpdir")}/emma/cinema/imdb_written.csv").toString
      // Read it, write it, and then read it again
      val imdb = DataBag.readCSV[ImdbMovie]("file://" + inputPath, CSV())
      imdb.writeCSV("file://" + outputPath, CSV())
      DataBag.readCSV[ImdbMovie]("file://" + outputPath, CSV())
    })
  }

  // --------------------------------------------------------------------------
  // Miscellaneous
  // --------------------------------------------------------------------------

  "Miscellaneous" - {
    "Pattern matching in `yield`" in verify(u.reify {
      val range = DataBag(zipWithIndex(0 to 100))
      val squares = for (xy <- range) yield xy match { case (x, y) => x + y }
      squares.sum
    })

    "Map with partial function" in verify(u.reify {
      val range = DataBag(zipWithIndex(0 to 100))
      val squares = range map { case (x, y) => x + y }
      squares.sum
    })

    "Destructuring of a generator" in verify(u.reify {
      val range = DataBag(zipWithIndex(0 to 100))
      val squares = for { (x, y) <- range } yield x + y
      squares.sum
    })

    "Intermediate value definition" in verify(u.reify {
      val range = DataBag(zipWithIndex(0 to 100))
      val squares = for (xy <- range; sqr = xy._1 * xy._2) yield sqr
      squares.sum
    })

    "Root package capture" in verify(u.reify {
      val eu = "eu"
      val com = "com"
      val java = "java"
      val org = "org"
      val scala = "scala"
      DataBag(0 to 100).sum
    })

    "Constant expressions" in verify(u.reify {
      val as = for { x <- DataBag(1 to 100) } yield 1 // map
      val bs = DataBag(101 to 200) flatMap { _ => DataBag(2 to 4) } // flatMap
      val cs = for { x <- DataBag(201 to 300) if 5 == 1 } yield 5 // filter
      val ds = DataBag(301 to 400) withFilter { _ => true } // filter
      as union bs union cs union ds
    })

    "Updated tmp sink (sieve of Eratosthenes)" in verify(u.reify {
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

      positive union negative
    })

    "val destructuring" in verify(u.reify {
      val resource = "file://" + materializeResource("/cinema/imdb.csv")
      val imdbTop100 = DataBag.readCSV[ImdbMovie](resource, CSV())
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
    })
  }
}

object BaseCodegenSpec {
  lazy val jabberwocky = fromPath(materializeResource("/lyrics/Jabberwocky.txt"))

  lazy val imdb = DataBag.readCSV[ImdbMovie]("file://" + materializeResource("/cinema/imdb.csv"), CSV()).fetch()

  lazy val cannes =
    DataBag.readCSV[FilmFestWinner]("file://" + materializeResource("/cinema/canneswinners.csv"), CSV()).fetch()

  lazy val berlin =
    DataBag.readCSV[FilmFestWinner]("file://" + materializeResource("/cinema/berlinalewinners.csv"), CSV()).fetch()

  val (jabberwockyEven, jabberwockyOdd) = jabberwocky
    .flatMap { _ split "\\W+" }
    .partition { _.length % 2 == 0 }

  // Workaround for https://issues.scala-lang.org/browse/SI-9933
  def zipWithIndex[A, Repr, That](coll: IterableLike[A, Repr])(
    implicit bf: CanBuildFrom[Repr, (A, Int), That]
  ): That = {
    val b = bf(coll.repr)
    var i = 0
    for (x <- coll) {
      b += ((x, i))
      i += 1
    }
    b.result()
  }
}
