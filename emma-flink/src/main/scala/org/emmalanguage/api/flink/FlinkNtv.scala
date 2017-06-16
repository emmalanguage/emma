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

import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.{ExecutionEnvironment => FlinkEnv}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

object FlinkNtv {

  type R = RuntimeContext

  import FlinkDataSet.typeInfoForType
  import Meta.Projections.ctagFor

  //----------------------------------------------------------------------------
  // Specialized combinators
  //----------------------------------------------------------------------------

  def iterate[A: Meta](xs: DataBag[A])(
    N: Int, body: DataBag[A] => DataBag[A]
  )(
    implicit flink: FlinkEnv
  ): DataBag[A] = xs match {
    case FlinkDataSet(us) => FlinkDataSet(us.iterate(N)(unlift(body)))
  }

  //----------------------------------------------------------------------------
  // Broadcast support
  //----------------------------------------------------------------------------

  def broadcast[A: Meta, B: Meta](xs: DataBag[A], ys: DataBag[B])(
    implicit flink: FlinkEnv
  ): DataBag[A] = (xs, ys) match {
    case (FlinkDataSet(us), FlinkDataSet(vs)) =>
      us.withBroadcastSet(vs, ys.uuid.toString)
      xs
  }

  def bag[A: Meta](xs: DataBag[A])(ctx: R): DataBag[A] =
    ctx.getBroadcastVariableWithInitializer(
      xs.uuid.toString,
      new BroadcastVariableInitializer[A, DataBag[A]] {
        override def initializeBroadcastVariable(data: java.lang.Iterable[A]) = {
          val ys = Vector.newBuilder[A]
          val it = data.iterator()
          while (it.hasNext) ys += it.next()
          DataBag(ys.result())
        }
      })

  def map[A: Meta, B: Meta](h: R => A => B)(xs: DataBag[A])(
    implicit flink: FlinkEnv
  ): DataBag[B] = xs match {
    case FlinkDataSet(us) => FlinkDataSet(us.map(new RichMapFunction[A, B] {
      var f: A => B = _

      override def open(parameters: Configuration): Unit =
        f = h(getRuntimeContext)

      def map(x: A): B =
        f(x)
    }))
  }

  def flatMap[A: Meta, B: Meta](h: R => A => DataBag[B])(xs: DataBag[A])(
    implicit flink: FlinkEnv
  ): DataBag[B] = xs match {
    case FlinkDataSet(us) => FlinkDataSet(us.flatMap(new RichFlatMapFunction[A, B] {
      var f: A => DataBag[B] = _

      override def open(parameters: Configuration): Unit =
        f = h(getRuntimeContext)

      def flatMap(x: A, out: Collector[B]): Unit =
        f(x).collect().foreach(out.collect)
    }))
  }

  def filter[A: Meta](h: R => A => Boolean)(xs: DataBag[A])(
    implicit flink: FlinkEnv
  ): DataBag[A] = xs match {
    case FlinkDataSet(us) => FlinkDataSet(us.filter(new RichFilterFunction[A] {
      var p: A => Boolean = _

      override def open(parameters: Configuration): Unit =
        p = h(getRuntimeContext)

      def filter(x: A): Boolean =
        p(x)
    }))
  }

  //----------------------------------------------------------------------------
  // Helper Objects and Methods
  //----------------------------------------------------------------------------

  private def unlift[A: Meta](f: DataBag[A] => DataBag[A])(
    implicit flink: FlinkEnv
  ): DataSet[A] => DataSet[A] = xs => f(FlinkDataSet(xs)) match {
    case FlinkDataSet(ys) => ys
  }
}
