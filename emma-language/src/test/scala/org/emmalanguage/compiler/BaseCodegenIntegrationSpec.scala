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
import test.schema.Graphs._
import test.schema.Math._
import test.schema.Movies._
import test.util._

import org.scalatest._
import org.scalatest.prop.PropertyChecks
import org.scalactic.Equality

import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom
import scala.util.Random

import java.io.File
import java.nio.file.Paths

abstract class BaseCodegenIntegrationSpec extends FreeSpec
  with Matchers
  with PropertyChecks
  with BeforeAndAfter
  with DataBagEquality
  with RuntimeCompilerAware {

  import BaseCodegenIntegrationSpec._

  import compiler._

  // ---------------------------------------------------------------------------
  // abstract trait methods
  // ---------------------------------------------------------------------------

  /** A function providing a backend context instance which lives for the duration of `f`. */
  def withBackendContext[T](f: Env => T): T

  val inputDir = new File(tempPath("test/input"))
  val outputDir = new File(tempPath("test/output"))
  implicit val imdbMovieCSVConverter = CSVConverter[ImdbMovie]

  lazy val actPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      transformations(loadConfig(baseConfig)) :+ addContext: _*
    ).compose(_.tree)

  lazy val expPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      addContext
    ).compose(_.tree)

  def verify[T: Equality](e: u.Expr[T]): Unit = {
    val actTree = actPipeline(e)
    val expTree = expPipeline(e)
    val actRslt = withBackendContext(eval[Env => T](actTree))
    val expRslt = withBackendContext(eval[Env => T](expTree))
    actRslt shouldEqual expRslt
  }

  def cancelIfLabyrinth(): Unit = {
    assume(this.getClass.getSimpleName != "LabyrinthCodegenIntegrationSpec", "Ignored for Labyrinth")
  }

  def ignoreForLabyrinth(test: =>Unit): Unit = {
    cancelIfLabyrinth()
    test
  }

  def show(x: Any): String = x match {
    case _: DataBag[_] => x.asInstanceOf[DataBag[_]].collect().toString
    case _ => x.toString
  }

  before {
    // make sure that the base paths exist
    inputDir.mkdirs()
    outputDir.mkdirs()
  }

  after {
    deleteRecursive(outputDir)
    deleteRecursive(inputDir)
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
    // Ignored because the Labyrinth compilation doesn't yet handle closures
    "primitives" in ignoreForLabyrinth(verify(u.reify {
      val us = DataBag(1 to 3)
      val vs = DataBag(4 to 6)
      val ws = DataBag(7 to 9)
      for {
        x <- DataBag(2 to 20 by 2)
      } yield {
        if (us.exists(_ == x)) 9 * x
        else if (vs.exists(_ == x)) 5 * x
        else if (ws.exists(_ == x)) 1 * x
        else 0
      }
    }))

    // Ignored because it freezes for some reason //fixme: why?
    "tuples" in ignoreForLabyrinth(verify(u.reify {
      for { edge <- DataBag((1, 4, "A") :: (2, 5, "B") :: (3, 6, "C") :: Nil) }
        yield if (edge._1 < edge._2) edge._1 -> edge._2 else edge._2 -> edge._1
    }))

    // Ignored because it freezes for some reason //fixme: why?
    "case classes" in {
      ignoreForLabyrinth(verify(u.reify {
        for { edge <- DataBag(graph) } yield
          if (edge.label == "B") LabelledEdge(edge.dst, edge.src, "B")
          else edge.copy(label = "Y")
      }))
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

    // Ignored because of a bug in the Labyrinth compilation:
    // When there are nested lambdas, the compilation gets the inner one out, regardless of the inner one referring to
    // locals of the outer one.
    "comprehension with correlated result" in ignoreForLabyrinth(verify(u.reify {
      for {
        line <- DataBag(jabberwocky)
        word <- DataBag(line split "\\W+")
      } yield (line, word)
    }))
  }

  // --------------------------------------------------------------------------
  // Distinct and Union
  // --------------------------------------------------------------------------

  // Distinct is not yet supported in the Labyrinth compilation
  "Distinct" - {
    "strings" in ignoreForLabyrinth(verify(u.reify {
      DataBag(jabberwocky flatMap { _ split "\\W+" }).distinct
    }))

    "tuples" in ignoreForLabyrinth(verify(u.reify {
      DataBag(jabberwocky.flatMap { _ split "\\W+" } map {(_,1)}).distinct
    }))
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

    "multi-way on case classes with local input" in {
      // Q: how many Cannes or Berlinale winners are there in the IMDB top 100?
      verify(u.reify {
        val cannesTop100 = for {
          movie <- DataBag(imdb)
          winner <- DataBag(cannes)
          if (winner.title, winner.year) == (movie.title, movie.year)
        } yield (movie.year, winner.title)

        val berlinTop100 = for {
          movie <- DataBag(imdb)
          winner <- DataBag(berlin)
          if (winner.title, winner.year) == (movie.title, movie.year)
        } yield (movie.year, winner.title)

        cannesTop100 union berlinTop100
      })
    }
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

  // GroupBy is not yet supported in the Labyrinth compilation
  "Group" - {
    "materialization" in ignoreForLabyrinth(verify(u.reify {
      DataBag(Seq(1)) groupBy Predef.identity
    }))

    "materialization with closure" in ignoreForLabyrinth(verify(u.reify {
      val semiFinal = 8
      val bag = DataBag(new Random shuffle 0.until(100).toList)
      val top = for (g <- bag groupBy { _ % semiFinal })
        yield g.values.collect().sorted.take(semiFinal / 2).sum

      top.max
    }))
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
        val values = decade.values
        val total = values.size
        val avgRating = values.map { _.rating.toInt * 10 }.sum / (total * 10.0)
        val minRating = values.map { _.rating }.min
        val maxRating = values.map { _.rating }.max

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

    // Fixme: Bug in the Labyrinth compilation: the singSrc at the end should be a cross
    "with duplicate group names" in ignoreForLabyrinth(verify(u.reify {
      val movies = DataBag(imdb)

      val leastPopular = for {
        Group(decade, dmovies) <- movies groupBy { _.year / 10 }
      } yield (decade, dmovies.size, dmovies.map { _.rating }.min)

      val mostPopular = for {
        Group(decade, dmovies) <- movies groupBy { _.year / 10 }
      } yield (decade, dmovies.size, dmovies.map { _.rating }.max)

      (leastPopular, mostPopular)
    }))

    // GroupBy is not yet supported in the Labyrinth compilation
    "with multiple groups in the same comprehension" in ignoreForLabyrinth(verify(u.reify {
      for {
        can10 <- DataBag(cannes) groupBy { _.year / 10 }
        ber10 <- DataBag(berlin) groupBy { _.year / 10 }
        if can10.key == ber10.key
      } yield (can10.values.size, ber10.values.size)
    }))
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

  // --------------------------------------------------------------------------
  // MutableBag
  // --------------------------------------------------------------------------

  "MutableBag" - {
    "create and collect" in {
      cancelIfLabyrinth() // MutableBag is not supported in the Labyrinth compilation

      val act = withBackendContext(eval[Env => Seq[(Int, Long)]](actPipeline(u.reify(
        MutableBag(DataBag((1 to 100).map(x => x -> x.toLong))).bag().collect()
      ))))

      val exp = (1 to 100).map(x => x -> x.toLong)

      exp should contain theSameElementsAs act
    }

    "update and copy" in {
      cancelIfLabyrinth() // MutableBag is not supported in the Labyrinth compilation

      val exp1 = (1 to 10).map(x => x -> (if (x % 2 == 0) 2L * x else x))
      val exp2 = (1 to 10).map(x => x -> (if (x % 2 == 0) 2L * x else x))
      val exp3 = (1 to 10).map(x => x -> x.toLong)
      val exp4 = (1 to 10).map(x => x -> (if (x % 2 == 0) 2L * x else x))
      val exp5 = (1 to 10).map(x => x -> (if (x % 2 == 0) 2L * x else x))
      val exp6 = (1 to 10).map(x => x -> (if (x % 2 != 0) 2L * x else x))

      val act1 :: act2 :: act3 :: act4 :: act5 :: act6 :: Nil =
        withBackendContext(eval[Env => List[Seq[(Int, Long)]]](actPipeline(u.reify {
          val inputs = DataBag((1 to 10).map(x => x -> x.toLong))
          val state1 = MutableBag(inputs)
          val state2 = state1
          val state3 = state1.copy()

          state1.update(
            inputs.withFilter(_._1 % 2 == 0).groupBy(_._1)
          )((_, vOld, m) => vOld.map(_ + m.map(_._2).sum))

          val act1 = state1.bag().collect()
          val act2 = state2.bag().collect()
          val act3 = state3.bag().collect()

          state3.update(
            inputs.withFilter(_._1 % 2 != 0).groupBy(_._1)
          )((_, vOld, m) => vOld.map(_ + m.map(_._2).sum))

          val act4 = state1.bag().collect()
          val act5 = state2.bag().collect()
          val act6 = state3.bag().collect()

          act1 :: act2 :: act3 :: act4 :: act5 :: act6 :: Nil
        })))

      act1 should contain theSameElementsAs exp1
      act2 should contain theSameElementsAs exp2
      act3 should contain theSameElementsAs exp3

      act4 should contain theSameElementsAs exp4
      act5 should contain theSameElementsAs exp5
      act6 should contain theSameElementsAs exp6
    }
  }

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
        y <- DataBag(100 to 200)
        if x < y || x + y < 100 && x % 2 == 0 || y / 2 == 0
      } yield y + x
    })

    "of filters with UDF predicates" in verify(u.reify {
      for {
        x <- DataBag(1 to 1000)
        if !(p1(x) || (p2(x) && p3(x))) || (p1(x) || !p2(x))
      } yield x
    })

    "of names of case classes" in verify(u.reify {
      val movies = DataBag(imdb)
      val years = for (mov <- movies) yield ImdbYear(mov.year)
      years forall { case iy @ ImdbYear(yr) => iy == ImdbYear(yr) }
    })

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
    // Bug in the Labyrinth compilation
    "read/write case classes" in ignoreForLabyrinth(verify(u.reify {
      val inputPath = materializeResource("/cinema/imdb.csv")
      val outputPath = Paths.get(s"${System.getProperty("java.io.tmpdir")}/emma/cinema/imdb_written.csv").toString
      // Read it, write it, and then read it again
      val imdb = DataBag.readCSV[ImdbMovie]("file://" + inputPath, CSV())
      imdb.writeCSV("file://" + outputPath, CSV())
      DataBag.readCSV[ImdbMovie]("file://" + outputPath, CSV()).collect().sortBy(_.title)
    }))
  }

  // --------------------------------------------------------------------------
  // Miscellaneous
  // --------------------------------------------------------------------------

  "Miscellaneous" - {
    "Pattern matching in `yield`" in verify(u.reify {
      val range = DataBag(zipWithIndex(0 to 100))
      val squares = for (ij <- range) yield ij match { case (i, j) => i + j }
      squares.sum
    })

    "Map with partial function" in verify(u.reify {
      val range = DataBag(zipWithIndex(0 to 100))
      val squares = range map { case (i, j) => i + j }
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

    //noinspection ScalaUnusedSymbol
    "Root package capture" in verify(u.reify {
      val eu = "eu"
      val com = "com"
      val java = "java"
      val org = "org"
      val scala = "scala"
      DataBag(0 to 100).sum
    })

    "Constant expressions" in verify(u.reify {
      val as = for { _ <- DataBag(1 to 100) } yield 1 // map
      val bs = DataBag(101 to 200) flatMap { _ => DataBag(2 to 4) } // flatMap
      val cs = for { _ <- DataBag(201 to 300) if 5 == 1 } yield 5 // filter
      val ds = DataBag(301 to 400) withFilter { _ => true } // filter
      as union bs union cs union ds
    })

    // Ignored because the Labyrinth compilation doesn't yet handle closures
    "Updated tmp sink (sieve of Eratosthenes)" in ignoreForLabyrinth(verify(u.reify {
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
    }))

    // Bug in the Labyrinth compilation
    "val destructuring" in ignoreForLabyrinth(verify(u.reify {
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
    }))
  }
}

object BaseCodegenIntegrationSpec {
  lazy val jabberwocky = fromPath(materializeResource("/lyrics/Jabberwocky.txt"))

  lazy val imdb = DataBag.readCSV[ImdbMovie]("file://" + materializeResource("/cinema/imdb.csv"), CSV()).collect()

  lazy val cannes =
    DataBag.readCSV[FilmFestWinner]("file://" + materializeResource("/cinema/canneswinners.csv"), CSV()).collect()

  lazy val berlin =
    DataBag.readCSV[FilmFestWinner]("file://" + materializeResource("/cinema/berlinalewinners.csv"), CSV()).collect()

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
