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
package api.backend

import api._
import api.alg._

import scala.collection.Map

/**
 * Operators added by backend-agnostic transformations.
 *
 * Do not use those directly unless you want to hardcode physical execution aspects such as
 * join order and caching and you know exactly what you are doing.
 */
object LocalOps extends ComprehensionCombinators[LocalEnv] with Runtime[LocalEnv] {

  //--------------------------------------------------------
  // ComprehensionCombinators
  //--------------------------------------------------------

  /** Naive `cross` node. */
  def cross[A: Meta, B: Meta](
    xs: DataBag[A], ys: DataBag[B]
  )(implicit env: LocalEnv): DataBag[(A, B)] = for {
    x <- xs
    y <- ys
  } yield (x, y)

  /** Naive `equiJoin` node. */
  def equiJoin[A: Meta, B: Meta, K: Meta](
    kx: A => K, ky: B => K)(xs: DataBag[A], ys: DataBag[B]
  )(implicit env: LocalEnv): DataBag[(A, B)] = for {
    x <- xs
    y <- ys
    if kx(x) == ky(y)
  } yield (x, y)

  //--------------------------------------------------------
  // Runtime
  //--------------------------------------------------------

  /** Implement the underlying logical semantics only (identity function). */
  def cache[A: Meta](xs: DataBag[A])(implicit env: LocalEnv): DataBag[A] = xs

  /** Fuse a groupBy and a subsequent fold into a single operator. */
  def foldGroup[A: Meta, B: Meta, K: Meta](
    xs: DataBag[A], key: A => K, alg: Alg[A, B]
  )(implicit env: LocalEnv): DataBag[Group[K, B]] = ScalaSeq(xs.collect()
    .foldLeft(Map.empty[K, B]) { (acc, x) =>
      val k = key(x)
      val s = alg.init(x)
      val u = acc.get(k).fold(s)(alg.plus(_, s))
      acc + (k -> u)
    }.map(x => Group(x._1, x._2)).toSeq)
}
