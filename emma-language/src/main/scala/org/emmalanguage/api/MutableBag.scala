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
package api


import scala.collection.breakOut

/** A mutable bag abstraction. */
trait MutableBag[K, V] {

  /* The update function type. */
  type UpdateFunction[M] = (K, Option[V], M) => Option[V]

  /**
   * Performs an update operation with the following semantics.
   *
   * For each `m` in `ms`, the implementation will call
   *
   * - `f(m.key, Some(v), m.value)` if `(m.key, v)` is in the underlying mutable bag;
   * - `f(m.key, None, m.value)` if `(m.key, v)` is not in the underlying mutable bag;
   *
   * Each `f` call can optionally return an `v` value to be associated with the current `k`.
   *
   * The collection of updated values is merged in the underlying mutable bag and
   * returned as final result.
   *
   * The functional semantics of the function rely on the assumption that `K` is a
   * unique key in `ms`. Otherwise, the implied state update is non-deterministic.
   */
  def update[M: Meta](ms: DataBag[Group[K, M]])(f: UpdateFunction[M]): DataBag[(K, V)]

  /** Returns the underlying data of this `MutableBag` as an (immutable) `DataBag`. */
  def bag(): DataBag[(K, V)]

  /** Creates a copy of this `MutableBag`. Can be used for by-value assignment. */
  def copy(): MutableBag[K, V]
}

/** A mutable bag companion abstraction. */
object MutableBag {

  def apply[K: Meta, V: Meta](init: DataBag[(K, V)]): MutableBag[K, V] =
    new MutableBag[K, V] {

      /* Local state - a (mutable) reference to an (immutable) map of type `(K, V)`. */
      private var sm: Map[K, V] = init.collect().map(identity[(K, V)])(breakOut)

      def update[M: Meta](ms: DataBag[Group[K, M]])(f: UpdateFunction[M]): DataBag[(K, V)] = {
        val delta = for {
          m <- ms.collect()
          v <- f(m.key, sm.get(m.key), m.values)
        } yield m.key -> v

        sm = sm ++ delta

        DataBag(delta)
      }

      def bag(): DataBag[(K, V)] =
        DataBag(sm.toSeq)

      def copy(): MutableBag[K, V] =
        MutableBag(bag())
    }
}
