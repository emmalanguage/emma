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

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment => FlinkEnv}
import org.apache.flink.core.fs.FileSystem

import scala.language.{higherKinds, implicitConversions}

/** A `DataBag` implementation backed by a Flink `DataSet`. */
class FlinkDataSet[A: Meta] private[api](@transient private val rep: DataSet[A]) extends DataBag[A] {

  import FlinkDataSet.{typeInfoForType, wrap}

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
    case dataset: FlinkDataSet[A] => this.rep union dataset.rep
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

  def fetch(): Seq[A] =
    rep.collect()
}

object FlinkDataSet {

  import org.apache.flink.api.common.typeinfo.TypeInformation

  import util.Toolbox.universe._
  import util.Toolbox.{mirror, toolbox}

  private lazy val flinkApi = mirror.staticModule("org.apache.flink.api.scala.package")
  private lazy val typeInfo = flinkApi.info.decl(TermName("createTypeInformation"))

  private lazy val memo = collection.mutable.Map.empty[Any, Any]

  implicit def typeInfoForType[T: Meta]: TypeInformation[T] = {
    val ttag = implicitly[Meta[T]].ttag
    memo.getOrElseUpdate(
      ttag,
      toolbox.eval(q"$typeInfo[${ttag.tpe}]")).asInstanceOf[TypeInformation[T]]
  }

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  def apply[A: Meta](implicit flink: FlinkEnv): FlinkDataSet[A] =
    flink.fromElements[A]()

  def apply[A: Meta](seq: Seq[A])(implicit flink: FlinkEnv): FlinkDataSet[A] =
    flink.fromCollection(seq)

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
}