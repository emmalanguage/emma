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
package lib.graphs

import api._

@emma.lib
object transitiveClosure {

  def apply[V: Meta](edges: DataBag[Edge[V]]): DataBag[Edge[V]] = {
    var paths = edges.distinct
    var count = paths.size
    var added = 0L

    do {
      val delta = for {
        e1 <- paths
        e2 <- paths
        if e1.dst == e2.src
      } yield Edge(e1.src, e2.dst)

      paths = (paths union delta).distinct

      added = paths.size - count
      count = paths.size
    } while (added > 0)

    paths
  }
}
