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

import compiler.RuntimeCompiler
import io.csv._
import io.parquet._

import org.apache.flink.api.java.io.TypeSerializerInputFormat
import org.apache.flink.api.java.io.TypeSerializerOutputFormat
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.{ExecutionEnvironment => FlinkEnv}
import org.apache.flink.core.fs.FileSystem

import scala.language.higherKinds
import scala.language.implicitConversions
import scala.util.hashing.MurmurHash3

import java.net.URI

/** A `DataBag` implementation backed by a Flink `DataSet`. */
class FlinkDataSet[A: Meta] private[api](@transient private[api] val rep: DataSet[A]) extends DataBag[A] {

  import FlinkDataSet.typeInfoForType
  import FlinkDataSet.wrap
  import Meta.Projections._

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

  override def map[B: Meta](f: (A) => B): DataBag[B] =
    rep.map(f)

  override def flatMap[B: Meta](f: (A) => DataBag[B]): DataBag[B] =
    rep.flatMap((x: A) => f(x).fetch())

  def withFilter(p: (A) => Boolean): DataBag[A] =
    rep.filter(p)

  // -----------------------------------------------------
  // Grouping
  // -----------------------------------------------------

  override def groupBy[K: Meta](k: (A) => K): DataBag[Group[K, DataBag[A]]] =
    rep.groupBy(k).reduceGroup((it: Iterator[A]) => {
      val buffer = it.toBuffer // This is because the iterator might not be serializable
      Group(k(buffer.head), DataBag(buffer))
    })

  // -----------------------------------------------------
  // Set operations
  // -----------------------------------------------------

  override def union(that: DataBag[A]): DataBag[A] = that match {
    case dbag: ScalaSeq[A] => this union FlinkDataSet(dbag.rep)
    case dbag: FlinkDataSet[A] => this.rep union dbag.rep
    case _ => throw new IllegalArgumentException(s"Unsupported rhs for `union` of type: ${that.getClass}")
  }

  override def distinct: DataBag[A] =
    rep.distinct

  // -----------------------------------------------------
  // Sinks
  // -----------------------------------------------------

  override def writeCSV(path: String, format: CSV)(implicit converter: CSVConverter[A]): Unit = {
    require(format.charset == CSV.defaultCharset,
      s"""Unsupported `charset` value: `${format.charset}`""")
    require(format.escape == CSV.defaultEscape,
      s"""Unsupported `escape` character: '${format.escape.fold("")(_.toString)}'""")
    require(format.comment == CSV.defaultComment,
      s"""Unsupported `comment` character: '${format.comment.fold("")(_.toString)}'""")
    require(format.nullValue == CSV.defaultNullValue,
      s"""Unsupported `nullValue` string: "${format.nullValue}"""")

    rep.writeAsCsv(
      filePath = path,
      fieldDelimiter = format.delimiter.toString,
      writeMode = FileSystem.WriteMode.OVERWRITE
    )

    rep.getExecutionEnvironment.execute()
  }

  override def writeText(path: String): Unit =
    rep.writeAsText(
      filePath = path,
      writeMode = FileSystem.WriteMode.OVERWRITE
    )

  def writeParquet(path: String, format: Parquet)(implicit converter: ParquetConverter[A]): Unit =
    ???

  override def fetch(): Seq[A] = collect

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

object FlinkDataSet extends DataBagCompanion[FlinkEnv] {

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

  def empty[A: Meta](
    implicit flink: FlinkEnv
  ): DataBag[A] = flink.fromElements[A]()

  def apply[A: Meta](values: Seq[A])(
    implicit flink: FlinkEnv
  ): DataBag[A] = flink.fromCollection(values)

  def readText(path: String)(
    implicit flink: FlinkEnv
  ): DataBag[String] = flink.readTextFile(path)

  def readCSV[A: Meta : CSVConverter](path: String, format: CSV)(
    implicit flink: FlinkEnv
  ): DataBag[A] = {
    require(format.charset == CSV.defaultCharset,
      s"""Unsupported `charset` value: `${format.charset}`""")
    require(format.escape == CSV.defaultEscape,
      s"""Unsupported `escape` character: '${format.escape.fold("")(_.toString)}'""")
    require(format.nullValue == CSV.defaultNullValue,
      s"""Unsupported `nullValue` string: "${format.nullValue}"""")

    flink.readCsvFile[A](
      filePath = path,
      ignoreFirstLine = format.header,
      fieldDelimiter = s"${format.delimiter}",
      quoteCharacter = format.quote.getOrElse[Char]('"')
    )
  }

  def readParquet[A: Meta : ParquetConverter](path: String, format: Parquet)(
    implicit flink: FlinkEnv
  ): DataBag[A] = ???

  // ---------------------------------------------------------------------------
  // Implicit Rep -> DataBag conversion
  // ---------------------------------------------------------------------------

  implicit def wrap[A: Meta](rep: DataSet[A]): DataBag[A] =
    new FlinkDataSet(rep)

  // ---------------------------------------------------------------------------
  // ComprehensionCombinators
  // (these should correspond to `compiler.ir.ComprehensionCombinators`)
  // ---------------------------------------------------------------------------

  def cross[A: Meta, B: Meta](
    xs: DataBag[A], ys: DataBag[B]
  )(implicit flink: FlinkEnv): DataBag[(A, B)] = (xs, ys) match {
    case (xs: FlinkDataSet[A], ys: FlinkDataSet[B]) => xs.rep.cross(ys.rep)
  }

  def equiJoin[A: Meta, B: Meta, K: Meta](
    keyx: A => K, keyy: B => K)(xs: DataBag[A], ys: DataBag[B]
  )(implicit flink: FlinkEnv): DataBag[(A, B)] = {
    val rddOf = new DataSetExtractor(flink)
    (xs, ys) match {
      case (rddOf(xsDS), rddOf(ysDS)) =>
        (xsDS join ysDS) where keyx equalTo keyy
    }
  }

  private class DataSetExtractor(flink: FlinkEnv) {
    def unapply[A: Meta](bag: DataBag[A]): Option[DataSet[A]] = bag match {
      case bag: FlinkDataSet[A] => Some(bag.rep)
      case _ => Some(flink.fromCollection(bag.fetch()))
    }
  }

  // ---------------------------------------------------------------------------
  // RuntimeOps
  // ---------------------------------------------------------------------------

  def cache[A: Meta](xs: DataBag[A])(implicit flink: FlinkEnv): DataBag[A] =
    xs match {
      case xs: FlinkDataSet[A] =>
        val sinkName = sink(xs.rep)
        xs.env.execute(s"emma-cache-$sinkName")
        source[A](sinkName)
      case _ => xs
    }

  private[api] def sink[A: Meta](xs: DataSet[A])(implicit flink: FlinkEnv): String = {
    val typeInfo = typeInfoForType[A]
    val tempName = tempNames.next()
    val outFmt = new TypeSerializerOutputFormat[A]
    outFmt.setInputType(typeInfo, flink.getConfig)
    outFmt.setSerializer(typeInfo.createSerializer(flink.getConfig))
    xs.write(outFmt, tempPath(tempName), FileSystem.WriteMode.OVERWRITE)
    tempName
  }

  private[api] def source[A: Meta](fileName: String)(implicit flink: FlinkEnv): DataSet[A] = {
    val filePath = tempPath(fileName)
    val typeInfo = typeInfoForType[A]
    val inFmt = new TypeSerializerInputFormat[A](typeInfo)
    inFmt.setFilePath(filePath)
    flink.readFile(inFmt, filePath)
  }

  def foldGroup[A: Meta, B: Meta, K: Meta](
    xs: DataBag[A], key: A => K, sng: A => B, uni: (B, B) => B
  )(implicit flink: FlinkEnv): DataBag[(K, B)] = xs match {
    case xs: FlinkDataSet[A] => xs.rep
      .map(x => key(x) -> sng(x)).groupBy("_1")
      .reduce((x, y) => x._1 -> uni(x._2, y._2))
  }

  private val tempBase =
    new URI(System.getProperty("emma.flink.temp-base", "file:///tmp/emma/flink-temp/"))

  private[api] val tempNames = Stream.iterate(0)(_ + 1)
    .map(i => f"dataflow$i%03d")
    .toIterator

  private[api] def tempPath(tempName: String): String =
    tempBase.resolve(tempName).toURL.toString
}
