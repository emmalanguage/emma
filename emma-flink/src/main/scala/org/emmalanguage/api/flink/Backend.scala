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
import api.backend.ComprehensionCombinators
import api.backend.Runtime

import scala.language.higherKinds
import org.apache.flink.api.java.io.TypeSerializerInputFormat
import org.apache.flink.api.java.io.TypeSerializerOutputFormat
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.{ExecutionEnvironment => FlinkEnv}
import org.apache.flink.core.fs.FileSystem

import java.net.URI

/**
 * IR nodes added by backend-agnostic transformations.
 *
 * Do not use those directly unless you want to hardcode physical execution aspects such as
 * join order and caching you know exactly what you are doing.
 */
object Backend extends ComprehensionCombinators[FlinkEnv] with Runtime[FlinkEnv] {

  import FlinkDataSet.typeInfoForType
  import FlinkDataSet.wrap
  import Meta.Projections._

  // ---------------------------------------------------------------------------
  // ComprehensionCombinators
  // ---------------------------------------------------------------------------

  def cross[A: Meta, B: Meta](
    xs: DataBag[A], ys: DataBag[B]
  )(implicit flink: FlinkEnv): DataBag[(A, B)] = {
    val datasetOf = new DataSetExtractor(flink)
    (xs, ys) match {
      case (datasetOf(xsDS), datasetOf(ysDS)) => xsDS cross ysDS
    }
  }

  def equiJoin[A: Meta, B: Meta, K: Meta](
    keyx: A => K, keyy: B => K)(xs: DataBag[A], ys: DataBag[B]
  )(implicit flink: FlinkEnv): DataBag[(A, B)] = {
    val datasetOf = new DataSetExtractor(flink)
    (xs, ys) match {
      case (datasetOf(xsDS), datasetOf(ysDS)) =>
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
  // Runtime
  // ---------------------------------------------------------------------------

  def cache[A: Meta](xs: DataBag[A])(implicit flink: FlinkEnv): DataBag[A] =
    xs match {
      case xs: FlinkDataSet[A] =>
        val sinkName = sink(xs.rep)
        xs.env.execute(s"emma-cache-$sinkName")
        source[A](sinkName)
      case _ => xs
    }

  def foldGroup[A: Meta, B: Meta, K: Meta](
    xs: DataBag[A], key: A => K, sng: A => B, uni: (B, B) => B
  )(implicit flink: FlinkEnv): DataBag[(K, B)] = xs match {
    case xs: FlinkDataSet[A] => xs.rep
      .map(x => key(x) -> sng(x)).groupBy("_1")
      .reduce((x, y) => x._1 -> uni(x._2, y._2))
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

  private val tempBase =
    new URI(System.getProperty("emma.flink.temp-base", "file:///tmp/emma/flink-temp/"))

  private[emmalanguage] val tempNames = Stream.iterate(0)(_ + 1)
    .map(i => f"dataflow$i%03d")
    .toIterator

  private[emmalanguage] def tempPath(tempName: String): String =
    tempBase.resolve(tempName).toURL.toString

}
