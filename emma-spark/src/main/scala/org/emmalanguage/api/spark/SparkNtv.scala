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

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object SparkNtv {

  import Meta.Projections._

  implicit def encoderForType[T: Meta]: Encoder[T] =
    ExpressionEncoder[T]

  //----------------------------------------------------------------------------
  // Specialized combinators
  //----------------------------------------------------------------------------

  def select[A: Meta](p: String => Seq[Column])(xs: DataBag[A])(
    implicit s: SparkSession
  ): DataBag[A] = xs match {
    case dst(us) => dst(us.as("x").filter(and(p("x"))).as[A])
  }

  def project[A: Meta, B: Meta](f: String => Seq[Column])(xs: DataBag[A])(
    implicit s: SparkSession
  ): DataBag[B] = xs match {
    case dst(us) => dst(us.as("x").select(f("x"): _*).as[B])
  }

  def equiJoin[A: Meta, B: Meta, K: Meta](
    kx: String => Seq[Column], ky: String => Seq[Column])(xs: DataBag[A], ys: DataBag[B]
  )(implicit s: SparkSession): DataBag[(A, B)] = (xs, ys) match {
    case (dst(us), dst(vs)) => dst(us.joinWith(vs, and(zeq(kx("_1"), ky("_2")))))
  }

  //----------------------------------------------------------------------------
  // Helper Objects and Methods
  //----------------------------------------------------------------------------

  private def and(conjs: Seq[Column]): Column =
    conjs.reduce(_ && _)

  private def zeq(lhs: Seq[Column], rhs: Seq[Column]): Seq[Column] =
    for ((l, r) <- lhs zip rhs) yield l === r

  private object dst {
    def apply[A: Meta](
      rep: Dataset[A]
    )(implicit s: SparkSession): DataBag[A] = new SparkDataset(rep)

    def unapply[A: Meta](
      bag: DataBag[A]
    )(implicit s: SparkSession): Option[Dataset[A]] = bag match {
      case bag: SparkDataset[A] => Some(bag.rep)
      case bag: SparkRDD[A] => Some(s.createDataset(bag.rep))
      case _ => None
    }
  }

}
