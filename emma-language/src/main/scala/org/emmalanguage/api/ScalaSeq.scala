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

import alg.Alg

/** A `DataBag` implementation backed by a Scala `Seq`. */
class ScalaSeq[A] private[api]
(
  private[api] val rep: Seq[A]
)(
  @transient implicit val m: Meta[A]
) extends DataBag[A] {

  //@formatter:off
  /*@transient override val m: Meta[A] = new Meta[A] {
    override def ctag: ClassTag[A] = null
    override def ttag: TypeTag[A] = null
  }*/
  //@formatter:on

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](alg: Alg[A, B]): B =
    rep.foldLeft(alg.zero)((acc, x) => alg.plus(alg.init(x), acc))

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): DataBag[B] =
    ScalaSeq(rep.map(f))

  override def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B] =
    ScalaSeq(rep.flatMap(x => f(x).collect()))

  def withFilter(p: (A) => Boolean): DataBag[A] =
    ScalaSeq(rep.filter(p))

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    ScalaSeq(rep.groupBy(k).toSeq map { case (key, vals) => Group(key, ScalaSeq(vals)) })

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case dbag: ScalaSeq[A] => ScalaSeq(this.rep ++ dbag.rep)
    case _ => that union this
  }

  override def distinct: DataBag[A] =
    ScalaSeq(rep.distinct)

  // -----------------------------------------------------
  // Partition-based Ops
  // -----------------------------------------------------

  def sample(k: Int, seed: Long = 5394826801L): Vector[A] = {
    val sample = new collection.mutable.ArrayBuffer[A](k)
    val random = util.RanHash(seed)
    for ((e, i) <- rep.zipWithIndex) {
      if (i >= k) {
        val j = random.nextInt(i + 1)
        if (j < k) sample(j) = e
      } else sample += e
    }
    sample.toVector
  }

  def zipWithIndex(): DataBag[(A, Long)] =
    ScalaSeq(rep zip Stream.iterate(0L)(_ + 1))

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit =
    CSVScalaSupport(format).write(path)(rep)

  def writeText(path: String): Unit =
    TextSupport.write(path)(rep map (_.toString))

  def writeParquet(path: String, format: Parquet)(implicit converter: ParquetConverter[A]): Unit =
    ParquetScalaSupport(format).write(path)(rep)

  override def collect(): Seq[A] =
    rep
}

object ScalaSeq extends DataBagCompanion[LocalEnv] {

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def empty[A: Meta](
    implicit env: LocalEnv
  ): DataBag[A] = new ScalaSeq(Seq.empty)

  def apply[A: Meta](values: Seq[A])(
    implicit env: LocalEnv
  ): DataBag[A] = new ScalaSeq(values)

  def readText(path: String)(
    implicit env: LocalEnv
  ): DataBag[String] = new ScalaSeq(TextSupport.read(path).toStream)

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(
    implicit env: LocalEnv
  ): DataBag[A] = new ScalaSeq(CSVScalaSupport(format).read(path).toStream)

  def readParquet[A: Meta : ParquetConverter](path: String, format: Parquet)(
    implicit env: LocalEnv
  ): DataBag[A] = new ScalaSeq(ParquetScalaSupport(format).read(path).toStream)

  // This is used in the code generation in TranslateToDataflows when inserting `collect` calls
  def fromDataBag[A: Meta](bag: DataBag[A]): DataBag[A] =
    new ScalaSeq(bag.collect())

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  private[api] def unapply[A: Meta](
    bag: DataBag[A]
  ): Option[Seq[A]] = bag match {
    case bag: ScalaSeq[A] => Some(bag.rep)
  }
}
