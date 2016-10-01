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

import testschema.Literature._

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FreeSpec, Matchers}

import scala.language.{higherKinds, implicitConversions}

trait DataBagSpec extends FreeSpec with Matchers with PropertyChecks with DataBagEquality {

  // FIXME: intOrd should have a static owner
  // implicit val intOrd = implicitly[Ordering[Int]]
  import DataBagSpec.intOrd

  // ---------------------------------------------------------------------------
  // abstract trait methods
  // ---------------------------------------------------------------------------

  /** The [[DataBag]] refinement type under test (e.g. SparkRDD). */
  type Bag[A] <: DataBag[A]

  /** The type of the backend context associated with the refinement type (e.g. SparkContext). */
  type BackendContext

  /** A function providing a backend context instance which lives for the duration of `f`. */
  def withBackendContext[T](f: BackendContext => T): T

  /** An empty [[DataBag]] refinement type constructor. */
  def Bag[A: Meta](implicit ctx: BackendContext): Bag[A]

  /** An [[DataBag]] refinement type constructor which takes a Scala Seq. */
  def Bag[A: Meta](seq: Seq[A])(implicit ctx: BackendContext): Bag[A]

  // ---------------------------------------------------------------------------
  // spec tests
  // ---------------------------------------------------------------------------

  "structural recursion" in {
    withBackendContext { implicit sc =>
      val act = {
        val xs = Bag(hhCrts)
        val ys = Bag(hhCrts.map(DataBagSpec.f))

        // FIXME: these predicates have to be defined externally in order to be a serializable part of the fold closure
        // FIXME: However, hard-coding the expanded version of the problematic folds (find, count) resolves the issue
        val p1 = (c: Character) => c.name startsWith "Zaphod"
        val p2 = (c: Character) => c.name startsWith "Ford"
        val p3 = (c: Character) => c.name startsWith "Marvin"

        Seq(
          //@formatter:off
          "isEmpty"  -> xs.isEmpty,
          "nonEmpty" -> xs.nonEmpty,
          "min"      -> ys.fold(IntLimits.max)(x => x, (x, y) => intOrd.min(x, y)), // FIXME: ys.min does not work
          "max"      -> ys.fold(IntLimits.min)(x => x, (x, y) => intOrd.max(x, y)), // FIXME: ys.max does not work
          "sum"      -> ys.sum,
          "product"  -> ys.product,
          "size"     -> xs.size,
          "count"    -> xs.count(p1), // FIXME: fold macro needs externally defiend lambda
          "existsP"  -> xs.exists(_.name startsWith "Arthur"),
          "existsN"  -> xs.exists(_.name startsWith "Marvin"),
          "forallP"  -> xs.forall(_.name startsWith "Arthur"),
          "forallN"  -> xs.forall(_.name startsWith "Trillian"),
          "findP"    -> xs.find(p2), // FIXME: fold macro needs externally defiend lambda
          "findN"    -> xs.find(p3), // FIXME: fold macro needs externally defiend lambda
          "bottom"   -> ys.bottom(1),
          "top"      -> ys.top(2)
          //@formatter:on
        )
      }

      val exp = {
        val xs = hhCrts
        val ys = hhCrts.map(_.book.title.length)
        Seq(
          //@formatter:off
          "isEmpty"  -> xs.isEmpty,
          "nonEmpty" -> xs.nonEmpty,
          "min"      -> ys.min,
          "max"      -> ys.max,
          "sum"      -> ys.sum,
          "product"  -> ys.product,
          "size"     -> xs.size,
          "count"    -> xs.count(_.name startsWith "Zaphod"),
          "existsP"  -> xs.exists(_.name startsWith "Arthur"),
          "existsN"  -> xs.exists(_.name startsWith "Marvin"),
          "forallP"  -> xs.forall(_.name startsWith "Arthur"),
          "forallN"  -> xs.forall(_.name startsWith "Trillian"),
          "findP"    -> xs.find(_.name startsWith "Ford"),
          "findN"    -> xs.find(_.name startsWith "Marvin"),
          "bottom"   -> ys.sorted.slice(ys.length - 1, ys.length),
          "top"      -> ys.sorted.slice(0, 2)
          //@formatter:on
        )
      }

      act should have size exp.size

      for ((k, v) <- exp)
        act should contain(k, v)
    }
  }

  "monad ops" - {

    "map" in {
      withBackendContext { implicit sc =>
        val act = Bag(hhCrts)
          .map(c => c.name)

        val exp = hhCrts
          .map(c => c.name)

        act shouldEqual DataBag(exp)
      }
    }

    "flatMap" in {
      withBackendContext { implicit sc =>
        val act = Bag(Seq((hhBook, hhCrts)))
          .flatMap { case (b, cs) => DataBag(cs) }

        val exp = Seq((hhBook, hhCrts))
          .flatMap { case (b, cs) => cs }

        act shouldEqual DataBag(exp)
      }
    }

    "withFilter" in {
      withBackendContext { implicit spark =>
        val act = Bag(Seq(hhBook))
          .withFilter(_.title == "The Hitchhiker's Guide to the Galaxy")

        val exp = Seq(hhBook)
          .filter(_.title == "The Hitchhiker's Guide to the Galaxy")

        act shouldEqual DataBag(exp)
      }
    }

    "for-comprehensions" in {
      withBackendContext { implicit sc =>
        val act = for {
          b <- Bag(Seq(hhBook))
          c <- ScalaTraversable(hhCrts) // nested DataBag cannot be RDDDataBag, as those are not serializable
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
  }

  "grouping" in {
    withBackendContext { implicit sc =>
      val act = Bag(hhCrts).groupBy(_.book)

      val exp = hhCrts.groupBy(_.book).toSeq.map {
        case (k, vs) => Group(k, DataBag(vs))
      }

      act shouldEqual DataBag(exp)
    }
  }

  "set operations" - {

    val xs = Seq("foo", "bar", "baz", "boo", "buz", "baz", "bag")
    val ys = Seq("fuu", "bin", "bar", "bur", "lez", "liz", "lag")

    "union" in {
      withBackendContext { implicit sc =>
        val act = Bag(xs) union Bag(ys)
        val exp = xs union ys

        act shouldEqual DataBag(exp)
      }
    }

    "distinct" in {
      withBackendContext { implicit sc =>
        val acts = Seq(Bag(xs).distinct, Bag(ys).distinct)
        val exps = Seq(xs.distinct, ys.distinct)

        for ((act, exp) <- acts zip exps)
          act shouldEqual Bag(exp)
      }
    }
  }
}

object DataBagSpec {
  implicit val intOrd = implicitly[Ordering[Int]]

  val f = (c: Character) => c.book.title.length
}
