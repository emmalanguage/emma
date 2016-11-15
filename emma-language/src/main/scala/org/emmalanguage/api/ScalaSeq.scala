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

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import scala.language.{higherKinds, implicitConversions}

/** A `DataBag` implementation backed by a Scala `Seq`. */
class ScalaSeq[A] private[api](private[api] val rep: Seq[A]) extends DataBag[A] {

  import ScalaSeq.wrap

  //@formatter:off
  @transient override val m: Meta[A] = new Meta[A] {
    override def ctag: ClassTag[A] = null
    override def ttag: TypeTag[A] = null
  }
  //@formatter:on

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, p: (B, B) => B): B =
    rep.foldLeft(z)((acc, x) => p(s(x), acc))

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): ScalaSeq[B] =
    rep.map(f)

  override def flatMap[B: Meta](f: (A) => DataBag[B]): ScalaSeq[B] =
    rep.flatMap(x => f(x).fetch())

  def withFilter(p: (A) => Boolean): ScalaSeq[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): ScalaSeq[Group[K, DataBag[A]]] =
    wrap(rep.groupBy(k).toSeq map { case (key, vals) => Group(key, wrap(vals)) })

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case dbag: ScalaSeq[A] => this.rep ++ dbag.rep
    case _ => that union this
  }

  override def distinct: ScalaSeq[A] =
    rep.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  override def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit =
    CSVScalaSupport(format).write(path)(rep)

  override def fetch(): Seq[A] =
    rep

}

object ScalaSeq {

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def empty[A]: ScalaSeq[A] =
    new ScalaSeq(Seq.empty)

  def apply[A](values: Seq[A]): ScalaSeq[A] =
    new ScalaSeq(values)

  def readCSV[A: CSVConverter](path: String, format: CSV): ScalaSeq[A] =
    new ScalaSeq(CSVScalaSupport(format).read(path).toStream)

  // This is used in the code generation in TranslateToDataflows when inserting fetches
  def byFetch[A](bag: DataBag[A]): ScalaSeq[A] =
    new ScalaSeq(bag.fetch())

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  private implicit def wrap[A](rep: Seq[A]): ScalaSeq[A] =
    new ScalaSeq(rep)
}