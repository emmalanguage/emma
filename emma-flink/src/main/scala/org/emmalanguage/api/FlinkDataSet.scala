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

import io.csv.{CSV, CSVConverter}
import compiler.RuntimeCompiler

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment => FlinkEnv}
import org.apache.flink.core.fs.FileSystem

import scala.language.{higherKinds, implicitConversions}
import scala.util.hashing.MurmurHash3

/** A `DataBag` implementation backed by a Flink `DataSet`. */
class FlinkDataSet[A: Meta] private[api](@transient private val rep: DataSet[A]) extends DataBag[A] {

  import Meta.Projections._

  import FlinkDataSet.{typeInfoForType, wrap}

  @transient override val m = implicitly[Meta[A]]
  private implicit def env = this.rep.getExecutionEnvironment

  // -----------------------------------------------------
  // Structural recursion
  // -----------------------------------------------------

  override def fold[B: Meta](z: B)(s: A => B, u: (B, B) => B): B = {
    val collected = rep.map(x => s(x)).reduce(u).collect()
    assert(collected.size <= 1)
    if (collected.isEmpty) z
    else collected.head
  }

  // -----------------------------------------------------
  // Monad Ops
  // -----------------------------------------------------

  override def map[B: Meta](f: (A) => B): FlinkDataSet[B] =
    rep.map(f)

  override def flatMap[B: Meta](f: (A) => DataBag[B]): FlinkDataSet[B] =
    rep.flatMap((x: A) => f(x).fetch())

  def withFilter(p: (A) => Boolean): FlinkDataSet[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): FlinkDataSet[Group[K, DataBag[A]]] =
    rep.groupBy(k).reduceGroup((it: Iterator[A]) => {
      val buffer = it.toBuffer // This is because the iterator might not be serializable
      Group(k(buffer.head), DataBag(buffer))
    })

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): FlinkDataSet[A] = that match {
    case dbag: ScalaSeq[A] => this.rep union FlinkDataSet(dbag.rep).rep
    case dbag: FlinkDataSet[A] => this.rep union dbag.rep
    case _ => throw new IllegalArgumentException(s"Unsupported rhs for `union` of type: ${that.getClass}")
  }

  override def distinct: FlinkDataSet[A] =
    rep.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  override def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit = {
    require(format.charset == CSV.DEFAULT_CHARSET,
      s"""Unsupported `charset` value: `${format.charset}`""")
    require(format.escape == CSV.DEFAULT_ESCAPE,
      s"""Unsupported `escape` character: '${format.escape.map(_.toString).getOrElse("")}'""")
    require(format.comment == CSV.DEFAULT_COMMENT,
      s"""Unsupported `comment` character: '${format.comment.map(_.toString).getOrElse("")}'""")
    require(format.nullValue == CSV.DEFAULT_NULLVALUE,
      s"""Unsupported `nullValue` string: "${format.nullValue}"""")

    rep.writeAsCsv(
      filePath = path,
      fieldDelimiter = format.delimiter.toString,
      writeMode = FileSystem.WriteMode.OVERWRITE
    )

    rep.getExecutionEnvironment.execute()
  }

  def fetch(): Seq[A] = collect

  private lazy val collect: Seq[A] =
    rep.collect()

  // -----------------------------------------------------
  // equals and hashCode
  // -----------------------------------------------------

  override def equals(o: Any) =
    super.equals(o)

  override def hashCode(): Int = {
    val (a, b, c, n) = rep
      .mapPartition(it => {
        var a, b, n = 0
        var c = 1
        it foreach { x =>
          val h = x.##
          a += h
          b ^= h
          if (h != 0) c *= h
          n += 1
        }
        Some((a, b, c, n)).iterator
      })
      .collect()
      .fold((0, 0, 1, 0))((x, r) => (x, r) match {
        case ((a1, b1, c1, n1), (a2, b2, c2, n2)) => (a1 + a2, b1 ^ b2, c1 * c2, n1 + n2)
      })

    var h = MurmurHash3.traversableSeed
    h = MurmurHash3.mix(h, a)
    h = MurmurHash3.mix(h, b)
    h = MurmurHash3.mixLast(h, c)
    MurmurHash3.finalizeHash(h, n)
  }
}

object FlinkDataSet {

  import org.apache.flink.api.common.typeinfo.TypeInformation

  val compiler = new RuntimeCompiler()

  import compiler.u._

  private lazy val memo = collection.mutable.Map.empty[Any, Any]

  implicit def typeInfoForType[T: Meta]: TypeInformation[T] = {
    val ttag = implicitly[Meta[T]].ttag
    memo.getOrElseUpdate(
      ttag,
      compiler.eval[TypeInformation[T]](
        // The dynamic cast in the following line is necessary, because the compiler can't see statically that
        // these path-dependent types are actually the same.
        q"org.apache.flink.api.scala.createTypeInformation[${ttag.tpe.asInstanceOf[Type]}]")
    ).asInstanceOf[TypeInformation[T]]
  }

  import Meta.Projections._

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def empty[A: Meta](implicit flink: FlinkEnv): FlinkDataSet[A] =
    flink.fromElements[A]()

  def apply[A: Meta](values: Seq[A])(implicit flink: FlinkEnv): FlinkDataSet[A] =
    flink.fromCollection(values)

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(implicit flink: FlinkEnv): FlinkDataSet[A] = {
    require(format.charset == CSV.DEFAULT_CHARSET,
      s"""Unsupported `charset` value: `${format.charset}`""")
    require(format.escape == CSV.DEFAULT_ESCAPE,
      s"""Unsupported `escape` character: '${format.escape.map(_.toString).getOrElse("")}'""")
    require(format.nullValue == CSV.DEFAULT_NULLVALUE,
      s"""Unsupported `nullValue` string: "${format.nullValue}"""")

    flink.readCsvFile[A](
      filePath = path,
      ignoreFirstLine = format.header,
      fieldDelimiter = s"${format.delimiter}",
      quoteCharacter = format.quote.getOrElse[Char]('"')
    )
  }

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  implicit def wrap[A: Meta](rep: DataSet[A]): FlinkDataSet[A] =
    new FlinkDataSet(rep)

  // ---------------------------------------------------------------------------
  // ComprehensionCombinators
  // (these should correspond to `compiler.ir.ComprehensionCombinators`)
  // ---------------------------------------------------------------------------

  def cross[A : Meta, B : Meta]
    (xs: DataBag[A], ys: DataBag[B])
    (implicit flink: FlinkEnv): DataBag[(A,B)] = (xs, ys) match {
    case (xs: FlinkDataSet[A], ys: FlinkDataSet[B]) => xs.rep.cross(ys.rep)
  }

  def equiJoin[A : Meta, B : Meta, K : Meta]
    (keyx: A => K, keyy: B => K)
    (xs: DataBag[A], ys: DataBag[B])
    (implicit flink: FlinkEnv): DataBag[(A,B)] = (xs, ys) match {
    case (xs: FlinkDataSet[A], ys: FlinkDataSet[B]) =>
      xs.rep.join(ys.rep).where(keyx).equalTo(keyy)
  }
}