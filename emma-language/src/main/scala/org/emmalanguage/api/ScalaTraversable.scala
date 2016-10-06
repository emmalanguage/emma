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

import io.csv.{CSV, CSVConverter, CSVScalaSupport}

import scala.language.{higherKinds, implicitConversions}

/** A `DataBag` implementation backed by a Scala `Traversable`. */
class ScalaTraversable[A] private[api](private val rep: Traversable[A]) extends DataBag[A] {

  import ScalaTraversable.wrap

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, p: (B, B) => B): B =
    rep.foldLeft(z)((acc, x) => p(s(x), acc))

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): ScalaTraversable[B] =
    rep.map(f)

  override def flatMap[B: Meta](f: (A) => DataBag[B]): ScalaTraversable[B] =
    rep.flatMap(x => f(x).fetch())

  def withFilter(p: (A) => Boolean): ScalaTraversable[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): ScalaTraversable[Group[K, DataBag[A]]] =
    wrap(rep.groupBy(k).toSeq map { case (key, vals) => Group(key, wrap(vals)) })

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): ScalaTraversable[A] = that match {
    case rdd: ScalaTraversable[A] => this.rep ++ rdd.rep
  }

  override def distinct: ScalaTraversable[A] =
    rep.toSeq.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  override def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit =
    CSVScalaSupport(format).write(path)(rep)

  override def fetch(): Traversable[A] =
    rep

  // -----------------------------------------------------
  // equals, hashCode and toString
  // -----------------------------------------------------

  override def equals(o: Any) = o match {
    case that: ScalaTraversable[A] =>
      lazy val sizeEq = this.rep.size == that.rep.size
      lazy val diffEm = (this.rep.toSeq diff this.rep.toSeq).isEmpty
      sizeEq && diffEm
    case _ =>
      false
  }

  override def hashCode(): Int =
    scala.util.hashing.MurmurHash3.unorderedHash(rep)

  override def toString: String =
    rep.toString
}

object ScalaTraversable {

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def apply[A]: ScalaTraversable[A] =
    new ScalaTraversable(Seq.empty)

  def apply[A](values: Traversable[A]): ScalaTraversable[A] =
    new ScalaTraversable(values)

  def readCSV[A: CSVConverter](path: String, format: CSV): ScalaTraversable[A] =
    new ScalaTraversable(CSVScalaSupport(format).read(path).toStream)

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  private implicit def wrap[A](rep: Traversable[A]): ScalaTraversable[A] =
    new ScalaTraversable(rep)
}