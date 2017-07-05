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
package util

import scala.annotation.tailrec
import scala.collection.Map

/** Utilities for [[quiver.Graph]]s. */
object Graphs {

  /** Sorts a DAG in topological order, returning `None` if the graph is not a DAG. */
  def topoSort[A](predecessors: Map[A, Set[A]]): Option[Seq[A]] = {
    @tailrec def topoSort(predecessors: Map[A, Set[A]], done: Vector[A]): Option[Seq[A]] = {
      val (roots, nodes) = predecessors.partition(_._2.isEmpty)
      if (roots.isEmpty) {
        if (nodes.isEmpty) Some(done) else None
      } else {
        val keys = roots.keySet
        topoSort(nodes.mapValues(_ diff keys), done ++ keys)
      }
    }

    topoSort(predecessors, Vector.empty)
  }
}
