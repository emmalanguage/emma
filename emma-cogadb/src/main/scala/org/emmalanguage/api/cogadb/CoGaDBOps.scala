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
package api.cogadb

import api._
import api.alg._
import api.backend.ComprehensionCombinators
import api.backend.Runtime
import runtime.CoGaDB



/** Spark backend operators. */
object CoGaDBOps extends ComprehensionCombinators[CoGaDB] with Runtime[CoGaDB] {

  //import Meta.Projections._

  //implicit def encoderForType[T: Meta]: Encoder[T] =
  //  ExpressionEncoder[T]

  //----------------------------------------------------------------------------
  // ComprehensionCombinators
  //----------------------------------------------------------------------------

  def cross[A: Meta, B: Meta](
    xs: DataBag[A], ys: DataBag[B]
  )(implicit cogadb: CoGaDB): DataBag[(A, B)] = (xs, ys) match {
    case _ => ???
    //case (rdd(us), rdd(vs)) => rdd(us cartesian vs)
  }

  def equiJoin[A: Meta, B: Meta, K: Meta](
    kx: A => K, ky: B => K)(xs: DataBag[A], ys: DataBag[B]
  )(implicit cogadb: CoGaDB): DataBag[(A, B)] = (xs, ys) match {
    case _ => ???
    //case (rdd(us), rdd(vs)) => rdd((us.map(extend(kx)) join vs.map(extend(ky))).values)
  }

  private def extend[X, K](k: X => K): X => (K, X) =
    x => (k(x), x)

  //----------------------------------------------------------------------------
  // Runtime
  //----------------------------------------------------------------------------

  def cache[A: Meta](xs: DataBag[A])(implicit cogadb: CoGaDB): DataBag[A] =
    xs match {
        //materialize
      //case xs: SparkDataset[A] => SparkDataset.wrap(xs.rep.cache())
      //case xs: SparkRDD[A] => SparkRDD.wrap(xs.rep.cache())
      case _ => xs
    }

  def foldGroup[A: Meta, B: Meta, K: Meta](
    xs: DataBag[A], key: A => K, alg: Alg[A, B]
  )(implicit cogadb: CoGaDB): DataBag[Group[K, B]] = xs match {
    case _ => ???
    /*case rdd(us) => rdd(us
      .map(x => key(x) -> alg.init(x))
      .reduceByKey(alg.plus)
      .map(x => Group(x._1, x._2)))*/
  }

  //----------------------------------------------------------------------------
  // Helper Objects
  //----------------------------------------------------------------------------

  private object cogadbTable {
    def apply[A: Meta](
      rep: CoGaDBTable[A]
    )(implicit cogadb: CoGaDB): DataBag[A] = rep

    def unapply[A: Meta](
      bag: DataBag[A]
    )(implicit cogadb: CoGaDB): Option[CoGaDBTable[A]] = bag match {
      case bag: CoGaDBTable[A] => Some(new CoGaDBTable(bag.rep))//Some(bag.rep)
      //case _ => Some(cogadb.parallelize(bag.collect()))
    }
  }

}