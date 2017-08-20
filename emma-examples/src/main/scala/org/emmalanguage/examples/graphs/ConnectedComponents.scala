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
package examples.graphs

import api._
import model._

import Ordering.Implicits._

@emma.lib
object ConnectedComponents {

  def apply[V: Ordering : Meta](edges: DataBag[Edge[V]]): DataBag[LVertex[V, V]] = {
    val vertices = edges.map(_.src).distinct

    // initial state
    val state = MutableBag(vertices.map(v => v -> v))
    // collection of vertices whose label changed in the last iteration
    var delta = state.bag()

    while (delta.nonEmpty) {
      val messages = for {
        Tuple2(k, v) <- delta
        Edge(src, dst) <- edges
        if k == src
      } yield Message(dst, v)

      delta = state.update(
        messages.groupBy(_.tgt)
      )((_, vOpt, ms) => for {
        v <- vOpt
        m = ms.map(_.payload).max
        if m > v
      } yield m)
    }

    // return solution set
    for ((vid, cmp) <- state.bag()) yield LVertex(vid, cmp)
  }
}
