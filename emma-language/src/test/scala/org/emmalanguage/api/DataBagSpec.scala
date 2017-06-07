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
package api

import test.schema.Literature._
import test.util.tempPath

import org.scalatest.FreeSpec
import org.scalatest.Matchers
import org.scalatest.prop.PropertyChecks

import scala.language.higherKinds
import scala.language.implicitConversions
import scala.util.Random

import java.nio.file.Files
import java.nio.file.Paths

trait DataBagSpec extends FreeSpec with Matchers with PropertyChecks with DataBagEquality {

  import DataBagSpec._

  // indicates that the backend supports CSV
  val supportsCSV = true

  // indicates that the backend supports Parquet
  val supportsParquet = true

  // ---------------------------------------------------------------------------
  // abstract trait methods
  // ---------------------------------------------------------------------------

  /** The type of the backend context associated with the refinement type (e.g. SparkContext). */
  type BackendContext

  /** The [[DataBag]] refinement type under test (e.g. SparkRDD). */
  type TestBag[A] <: DataBag[A]

  /** The [[DataBag]] refinement companion type under test (e.g. SparkRDD). */
  val TestBag: DataBagCompanion[BackendContext]

  /** A system-specific suffix to append to test files. */
  def suffix: String

  /** A function providing a backend context instance which lives for the duration of `f`. */
  def withBackendContext[T](f: BackendContext => T): T

  // ---------------------------------------------------------------------------
  // spec tests
  // ---------------------------------------------------------------------------

  "structural recursion" in withBackendContext { implicit ctx =>
    val act = {
      val vs = TestBag(Seq((0, 0.0)))
      val ws = TestBag(Seq(0))
      val xs = TestBag(hhCrts)
      val ys = TestBag(hhCrts.map(DataBagSpec.f))
      val zs = TestBag(Seq.empty[Double])

      Seq(
        //@formatter:off
        "isEmpty"     -> xs.isEmpty,
        "nonEmpty"    -> xs.nonEmpty,
        "min (1)"     -> vs.min,
        "min (2)"     -> ws.min,
        "min (3)"     -> vs.min,
        "max (1)"     -> vs.max,
        "max (2)"     -> ws.max,
        "max (3)"     -> ys.max,
        "sum (1)"     -> ys.sum,
        "sum (2)"     -> zs.sum,
        "product (1)" -> ys.product,
        "product (2)" -> zs.product,
        "size (1)"    -> xs.size,
        "size (2)"    -> zs.size,
        "count (1)"   -> xs.count(_.name startsWith "Zaphod"),
        "count (2)"   -> zs.count(_ < 42.0),
        "existsP"     -> xs.exists(_.name startsWith "Arthur"),
        "existsN"     -> xs.exists(_.name startsWith "Marvin"),
        "forallP"     -> xs.forall(_.name startsWith "Arthur"),
        "forallN"     -> xs.forall(_.name startsWith "Trillian"),
        "findP"       -> xs.find(_.name startsWith "Ford"),
        "findN"       -> xs.find(_.name startsWith "Marvin"),
        "bottom"      -> ys.bottom(1),
        "top"         -> ys.top(2)
        //@formatter:on
      )
    }

    val exp = {
      val vs = TestBag(Seq((0, 0.0)))
      val ws = Seq(0)
      val xs = hhCrts
      val ys = hhCrts.map(_.book.title.length)
      val zs = Seq.empty[Double]

      Seq(
        //@formatter:off
        "isEmpty"     -> xs.isEmpty,
        "nonEmpty"    -> xs.nonEmpty,
        "min (1)"     -> vs.min,
        "min (2)"     -> ws.min,
        "min (3)"     -> vs.min,
        "max (1)"     -> vs.max,
        "max (2)"     -> ws.max,
        "max (3)"     -> ys.max,
        "sum (1)"     -> ys.sum,
        "sum (2)"     -> zs.sum,
        "product (1)" -> ys.product,
        "product (2)" -> zs.product,
        "size (1)"    -> xs.size,
        "size (2)"    -> zs.size,
        "count (1)"   -> xs.count(_.name startsWith "Zaphod"),
        "count (2)"   -> zs.count(_ < 42.0),
        "existsP"     -> xs.exists(_.name startsWith "Arthur"),
        "existsN"     -> xs.exists(_.name startsWith "Marvin"),
        "forallP"     -> xs.forall(_.name startsWith "Arthur"),
        "forallN"     -> xs.forall(_.name startsWith "Trillian"),
        "findP"       -> xs.find(_.name startsWith "Ford"),
        "findN"       -> xs.find(_.name startsWith "Marvin"),
        "bottom"      -> ys.sorted.slice(ys.length - 1, ys.length),
        "top"         -> ys.sorted.slice(0, 2)
        //@formatter:on
      )
    }


    withClue("Bag.empty[(Int, Double)].min throws `NoSuchElementException`") {
      intercept[NoSuchElementException](TestBag.empty[(Int, Double)].min)
    }
    withClue("Bag.empty[Int].max throws `NoSuchElementException`") {
      intercept[NoSuchElementException](TestBag.empty[Int].max)
    }

    act should have size exp.size

    for ((k, v) <- exp)
      act should contain(k, v)
  }

  "monad ops" - {
    "map" in withBackendContext { implicit ctx =>
      val act = TestBag(hhCrts)
        .map(c => c.name)

      val exp = hhCrts
        .map(c => c.name)

      act shouldEqual DataBag(exp)
    }

    "flatMap" in withBackendContext { implicit ctx =>
      val act = TestBag(Seq((hhBook, hhCrts)))
        .flatMap { case (_, cs) => DataBag(cs) }

      val exp = Seq((hhBook, hhCrts))
        .flatMap { case (_, cs) => cs }

      act shouldEqual DataBag(exp)
    }

    "withFilter" in withBackendContext { implicit spark =>
      val act = TestBag(Seq(hhBook))
        .withFilter(_.title == "The Hitchhiker's Guide to the Galaxy")

      val exp = Seq(hhBook)
        .filter(_.title == "The Hitchhiker's Guide to the Galaxy")

      act shouldEqual DataBag(exp)
    }

    "for-comprehensions" in withBackendContext { implicit ctx =>
      val act = for {
        b <- TestBag(Seq(hhBook))
        c <- ScalaSeq(hhCrts) // nested DataBag cannot be RDDDataBag, as those are not serializable
        if b.title == c.book.title
        if b.title == "The Hitchhiker's Guide to the Galaxy"
      } yield (b.title, c.name)

      val exp = for {
        b <- Seq(hhBook)
        c <- hhCrts
        if b.title == c.book.title
        if b.title == "The Hitchhiker's Guide to the Galaxy"
      } yield (b.title, c.name)

      act shouldEqual DataBag(exp)
    }
  }

  "grouping" in withBackendContext { implicit ctx =>
    val act = TestBag(hhCrts).groupBy(_.book)

    val exp = hhCrts.groupBy(_.book).toSeq.map {
      case (k, vs) => Group(k, DataBag(vs))
    }

    act shouldEqual DataBag(exp)
  }

  "set operations" - {
    val xs = Seq("foo", "bar", "baz", "boo", "buz", "baz", "bag")
    val ys = Seq("fuu", "bin", "bar", "bur", "lez", "liz", "lag")

    "union" in withBackendContext { implicit ctx =>
      val act = TestBag(xs) union TestBag(ys)
      val exp = xs union ys

      act shouldEqual DataBag(exp)
    }

    "distinct" in withBackendContext { implicit ctx =>
      val acts = Seq(TestBag(xs).distinct, TestBag(ys).distinct)
      val exps = Seq(xs.distinct, ys.distinct)

      for ((act, exp) <- acts zip exps)
        act shouldEqual TestBag(exp)
    }
  }

  "sample" - {
    val s1 = 54326427L
    val s2 = 23546473L

    "with k >= bag.size" in withBackendContext { implicit ctx =>
      val xs = TestBag(0 to 7)
      xs.sample(8) should contain theSameElementsAs (0 to 7)
      xs.sample(9) should contain theSameElementsAs (0 to 7)
    }

    "with k < bag.size" in withBackendContext { implicit ctx =>
      val xs = TestBag(0 to 7)
      xs.sample(1) shouldEqual xs.sample(1)
      xs.sample(7) shouldEqual xs.sample(7)
    }

    "with matching explicit seeds" in withBackendContext { implicit ctx =>
      val xs = TestBag(0 to 7)
      xs.sample(1, s1) shouldEqual xs.sample(1, s1)
      xs.sample(7, s1) shouldEqual xs.sample(7, s1)
    }

    "with non-matching explicit seeds" in withBackendContext { implicit ctx =>
      val xs = TestBag(0 to 7)
      xs.sample(1, s1) shouldNot equal(xs.sample(1, s2))
      xs.sample(4, s1) shouldNot equal(xs.sample(4, s2))
    }
  }

  "zipWithIndex" in withBackendContext { implicit ctx =>
    val xs = (('a' to 'z') ++ ('A' to 'Z')).map(_.toString)

    val act = TestBag(xs).zipWithIndex().map(_._2)
    val exp = 0L to 51L

    act shouldEqual DataBag(exp)
  }

  "empty" in withBackendContext(implicit ctx =>
    DataBag.empty[Nothing].isEmpty shouldBe true
  )

  "csv support" - {
    implicit val rand = new Random(4531442314L)
    val format = CSV()

    // ensure that output path exists
    Files.createDirectories(Paths.get(tempPath("test/io")))

    "Book" - {
      val exp = Seq(hhBook)
      "can read native output" ifSupportsCSV withBackendContext { implicit ctx =>
        val pat = path(s"books.native.csv")
        DataBag(exp).writeCSV(pat, format)
        TestBag.readCSV[Book](pat, format) shouldEqual DataBag(exp)
      }
      "can read backend output" ifSupportsCSV withBackendContext { implicit ctx =>
        val pat = path(s"books.$suffix.csv")
        TestBag(exp).writeCSV(pat, format)
        TestBag.readCSV[Book](pat, format) shouldEqual DataBag(exp)
      }
    }

    "Record" - {
      val exp = csvRecords()
      "can read native output" ifSupportsCSV withBackendContext { implicit ctx =>
        val pat = path(s"records.native.csv")
        DataBag(exp).writeCSV(pat, format)
        TestBag.readCSV[CSVRecord](pat, format) shouldEqual DataBag(exp)
      }
      "can read backend output" ifSupportsCSV withBackendContext { implicit ctx =>
        val pat = path(s"records.$suffix.csv")
        TestBag(exp).writeCSV(pat, format)
        TestBag.readCSV[CSVRecord](pat, format) shouldEqual DataBag(exp)
      }
    }
  }

  "parquet support" - {
    implicit val rand = new Random(4531442314L)
    val format = Parquet()

    // ensure that output path exists
    Files.createDirectories(Paths.get(tempPath("test/io")))

    "Book" - {
      val exp = Seq(hhBook)
      "can read native output" ifSupportsParquet withBackendContext { implicit ctx =>
        val pat = path(s"books.native.parquet")
        DataBag(exp).writeParquet(pat, format)
        TestBag.readParquet[Book](pat, format) shouldEqual DataBag(exp)
      }
      "can read backend output" ifSupportsParquet withBackendContext { implicit ctx =>
        val pat = path(s"books.$suffix.parquet")
        TestBag(exp).writeParquet(pat, format)
        TestBag.readParquet[Book](pat, format) shouldEqual DataBag(exp)
      }
    }

    "Record" - {
      val exp = parquetRecords()
      "can read native output" ifSupportsParquet withBackendContext { implicit ctx =>
        val pat = path(s"records.native.parquet")
        DataBag(exp).writeParquet(pat, format)
        TestBag.readParquet[ParquetRecord](pat, format).collect() should contain theSameElementsAs exp
      }
      "can read backend output" ifSupportsParquet withBackendContext { implicit ctx =>
        val pat = path(s"records.$suffix.parquet")
        TestBag(exp).writeParquet(pat, format)
        TestBag.readParquet[ParquetRecord](pat, format).collect() should contain theSameElementsAs exp
      }
    }
  }

  private def path(name: String): String =
    s"file://${tempPath("test/io")}/$name"

  private def csvRecords(
    quote: Option[Char] = None
  )(implicit r: Random): Seq[CSVRecord] =
    for (_ <- 1 to 1000) yield CSVRecord(
      //@formatter:off
      first       = r.nextInt(Int.MaxValue),
      second      = r.nextDouble(),
      third_field = r.nextBoolean(),
      fourth      = (r.nextInt() % (2 * Short.MaxValue) + Short.MinValue).toShort,
      // Fifth    = r.nextPrintableChar(),
      sixth       = randString(quote),
      seventh     = if (r.nextBoolean()) Some(r.nextLong()) else None,
      nine        = r.nextFloat()
      //@formatter:on
    )

  private def parquetRecords(
    quote: Option[Char] = None
  )(implicit r: Random): Seq[ParquetRecord] =
    for (_ <- 1 to 1) yield ParquetRecord(
      //@formatter:off
      title       = r.nextInt(5).toShort,
      name        = Name(
        first     = randString(quote),
        middle    = if (r.nextBoolean()) Some(randString(quote)) else None,
        last      = randString(quote)
      ),
      dvalue      = r.nextDouble(),
      svalue      = if (r.nextBoolean()) Some(r.nextInt(255).toShort) else None,
      measures    = (1 to r.nextInt(1024)).map(_ => r.nextLong())
      //@formatter:on
    )

  private def randString(
    quote: Option[Char] = None
  )(implicit r: Random): String = {
    val charsClean = r
      .alphanumeric
      .take(r.nextInt(50) + 50)
      .toList
      .map(c => if (c == '0') ' ' else c)

    val charsDirty = for {
      q <- quote
    } yield charsClean.map(c => if (c == 'q') q else c)

    (charsDirty getOrElse charsClean).mkString.trim
  }

  protected final class DataBagSpecStringWrapper(string: String) {
    def ifSupportsCSV(f: => Unit): Unit =
      if (supportsCSV) string in f
      else string ignore f

    def ifSupportsParquet(f: => Unit): Unit =
      if (supportsParquet) string in f
      else string ignore f
  }

  protected implicit def toDataBagSpecStringWrapper(s: String): DataBagSpecStringWrapper =
    new DataBagSpecStringWrapper(s)
}

object DataBagSpec {

  val f = (c: Character) => c.book.title.length

  //@formatter:off
  case class CSVRecord
  (
    first       : Int,
    second      : Double,
    third_field : Boolean,
    fourth      : Short,
    // FIXME: Spark does not support `Char` type
    // Fifth    = r.nextPrintableChar(),
    sixth       : String,
    seventh     : Option[Long],
    nine        : Float
  )

  case class ParquetRecord
  (
    title       : Short,
    name        : Name,
    dvalue      : Double,
    svalue      : Option[Short],
    measures    : Seq[Long]
  )

  case class Name
  (
    first       : String,
    middle      : Option[String],
    last        : String
  )
  //@formatter:on
}
