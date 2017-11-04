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

import FlinkMutableBag.State
import flink._

import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.util.Collector

class FlinkMutableBag[K: Meta, V: Meta] private
(
  /* Distributed state modeled naively as a mutable reference to an immutable bag. */
  var ss: DataBag[State[K, V]]
)(
  implicit flink: ExecutionEnvironment
) extends MutableBag[K, V] with Serializable {

  import FlinkDataSet.typeInfoForType

  def update[M: Meta](ms: DataBag[Group[K, M]])(f: UpdateFunction[M]): DataBag[(K, V)] = {
    val conv = implicitly[DataSet[State[K, V]] => DataBag[State[K, V]]]
    ss = FlinkOps.cache(conv((ss.as[DataSet] fullOuterJoin ms.as[DataSet]).where(0).equalTo(0)(
      (s: State[K, V], m: Group[K, M], out: Collector[State[K, V]]) => {
        val rs = Option(m) match {
          case Some(m) =>
            val vOld = Option(s)
            val vNew = f(m.key, vOld.map(_.v), m.values)
            vNew.map(State(m.key, _, true)) orElse vOld.map(_.copy(changed = false))
          case None =>
            Option(s).map(_.copy(changed = false))
        }
        rs.foreach(out.collect)
      })))
    for (s <- ss if s.changed) yield s.k -> s.v
  }

  def bag(): DataBag[(K, V)] =
    for (s <- ss) yield s.k -> s.v

  def copy(): MutableBag[K, V] =
    new FlinkMutableBag[K, V](ss)
}

object FlinkMutableBag {

  case class State[K, V](k: K, v: V, changed: Boolean = true)

  def apply[K: Meta, V: Meta](
    init: DataBag[(K, V)]
  )(
    implicit flink: ExecutionEnvironment
  ): MutableBag[K, V] = new FlinkMutableBag(FlinkOps.cache(for {
    (k, v) <- init
  } yield State(k, v)))

  private[api] val tempNames = Stream.iterate(0)(_ + 1)
    .map(i => f"stateful$i%03d")
    .toIterator
}
