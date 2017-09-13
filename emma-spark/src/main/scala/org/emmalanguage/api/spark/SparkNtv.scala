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
package api.spark

import api._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object SparkNtv {

  import SparkExp._

  //----------------------------------------------------------------------------
  // Specialized combinators
  //----------------------------------------------------------------------------

  def select[A: Meta](p: Root => Expr)(xs: DataBag[A])(
    implicit s: SparkSession
  ): DataBag[A] = xs match {
    case SparkDataset(us) =>
      val xs = us.as("x")
      val cx = p(Root("x")).col
      SparkDataset(xs.filter(cx))
  }

  def project[A: Meta, B: Meta](f: Root => Expr)(xs: DataBag[A])(
    implicit s: SparkSession
  ): DataBag[B] = xs match {
    case SparkDataset(us) =>
      import s.sqlContext.implicits._
      val fs = encoderForType[B].schema.fields
      val xs = us.as("x")
      val cx = f(Root("x")) match {
        case Chain(Seq(name)) =>
          if (fs.length > 1) Seq($"$name.*")
          else Seq($"$name.${fs(0).name}")
        case Chain(names) =>
          if (fs.length > 1) Seq($"${names.mkString(".")}.*")
          else Seq($"${names.mkString(".")}")
        case Struct(names, vals) =>
          for ((v, n) <- vals zip names) yield v.col.as(n)
        case expr =>
          Seq(expr.col)
      }
      SparkDataset(xs.select(cx: _*).as[B])
  }

  def equiJoin[A: Meta, B: Meta, K: Meta](
    kx: Root => Expr, ky: Root => Expr)(xs: DataBag[A], ys: DataBag[B]
  )(implicit s: SparkSession): DataBag[(A, B)] = (xs, ys) match {
    case (SparkDataset(us), SparkDataset(vs)) =>
      val xs = us.as("x")
      val ys = vs.as("y")
      val cx = kx(Root("x")).col
      val cy = ky(Root("y")).col
      SparkDataset(xs.joinWith(ys, cx === cy))
  }

  //----------------------------------------------------------------------------
  // Broadcast support
  //----------------------------------------------------------------------------

  class BroadcastBag[A](private[spark] val value: Broadcast[Seq[A]]) extends Serializable

  def broadcast[A: Meta](xs: DataBag[A])(
    implicit spark: SparkSession
  ): BroadcastBag[A] = {
    new BroadcastBag(spark.sparkContext.broadcast(xs.collect()))
  }

  def bag[A: Meta](bc: BroadcastBag[A]): DataBag[A] =
    DataBag(bc.value.value)
}
