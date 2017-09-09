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
package api.flink

import api._
import api.alg._
import api.backend.ComprehensionCombinators
import api.backend.Runtime

import org.apache.flink.api.java.io.TypeSerializerInputFormat
import org.apache.flink.api.java.io.TypeSerializerOutputFormat
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.{ExecutionEnvironment => FlinkEnv}
import org.apache.flink.core.fs.FileSystem

import java.net.URI

/** Flink backend operators. */
object FlinkOps extends ComprehensionCombinators[FlinkEnv] with Runtime[FlinkEnv] {

  import FlinkDataSet.typeInfoForType
  import Meta.Projections._

  // ---------------------------------------------------------------------------
  // ComprehensionCombinators
  // ---------------------------------------------------------------------------

  def cross[A: Meta, B: Meta](
    xs: DataBag[A], ys: DataBag[B]
  )(implicit flink: FlinkEnv): DataBag[(A, B)] = (xs, ys) match {
    case (FlinkDataSet(us), FlinkDataSet(vs)) => FlinkDataSet(us cross vs)
  }

  def equiJoin[A: Meta, B: Meta, K: Meta](
    kx: A => K, ky: B => K)(xs: DataBag[A], ys: DataBag[B]
  )(implicit flink: FlinkEnv): DataBag[(A, B)] = (xs, ys) match {
    case (FlinkDataSet(us), FlinkDataSet(vs)) => FlinkDataSet((us join vs) where kx equalTo ky)
  }

  // ---------------------------------------------------------------------------
  // Runtime
  // ---------------------------------------------------------------------------

  def cache[A: Meta](xs: DataBag[A])(implicit flink: FlinkEnv): DataBag[A] =
    xs match {
      case xs: FlinkDataSet[A] =>
        val sinkName = sink(xs.rep)
        xs.env.execute(s"emma-cache-$sinkName")
        FlinkDataSet(source[A](sinkName))
      case _ => xs
    }

  def foldGroup[A: Meta, B: Meta, K: Meta](
    xs: DataBag[A], key: A => K, alg: Alg[A, B]
  )(implicit flink: FlinkEnv): DataBag[Group[K, B]] = xs match {
    case xs: FlinkDataSet[A] => FlinkDataSet(xs.rep
      .map(x => Group(key(x), alg.init(x)))
      .groupBy("key")
      .reduce((x, y) => Group(x.key, alg.plus(x.values, y.values))))
  }

  private def sink[A: Meta](xs: DataSet[A])(implicit flink: FlinkEnv): String = {
    val typeInfo = typeInfoForType[A]
    val tempName = tempNames.next()
    val outFmt = new TypeSerializerOutputFormat[A]
    outFmt.setInputType(typeInfo, flink.getConfig)
    outFmt.setSerializer(typeInfo.createSerializer(flink.getConfig))
    xs.write(outFmt, tempPath(tempName), FileSystem.WriteMode.OVERWRITE)
    tempName
  }

  private def source[A: Meta](fileName: String)(implicit flink: FlinkEnv): DataSet[A] = {
    val filePath = tempPath(fileName)
    val typeInfo = typeInfoForType[A]
    val inFmt = new TypeSerializerInputFormat[A](typeInfo)
    inFmt.setFilePath(filePath)
    flink.readFile(inFmt, filePath)
  }

  lazy val tempBase =
    new URI(System.getProperty("emma.flink.temp-base", "file:///tmp/emma/flink-temp/"))

  private[emmalanguage] val tempNames = Stream.iterate(0)(_ + 1)
    .map(i => f"dataflow$i%03d")
    .toIterator

  private[emmalanguage] def tempPath(tempName: String): String =
    tempBase.resolve(tempName).toString

}
