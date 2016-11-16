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

import api.Meta
import api.emma

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Graph model objects. */
object model {

  case class Edge[V](src: V, dst: V)

  case class LEdge[V, L](@emma.pk src: V, @emma.pk dst: V, label: L)

  case class Triangle[V](x: V, y: V, z: V)

  //@formatter:off
  implicit def edgeMeta[V: Meta]: Meta[Edge[V]] = new Meta[Edge[V]] {
    implicit def ctagV = implicitly[Meta[V]].ctag
    implicit def ttagV = implicitly[Meta[V]].ttag
    override def ctag = implicitly[ClassTag[Edge[V]]]
    override def ttag = implicitly[TypeTag[Edge[V]]]
  }

  implicit def ledgeMeta[V: Meta, L: Meta]: Meta[LEdge[V, L]] = new Meta[LEdge[V, L]] {
    implicit def ctagV = implicitly[Meta[V]].ctag
    implicit def ttagV = implicitly[Meta[V]].ttag
    implicit def ctagL = implicitly[Meta[L]].ctag
    implicit def ttagL = implicitly[Meta[L]].ttag
    override def ctag = implicitly[ClassTag[LEdge[V, L]]]
    override def ttag = implicitly[TypeTag[LEdge[V, L]]]
  }

  implicit def triadMeta[V: Meta]: Meta[Triangle[V]] = new Meta[Triangle[V]] {
    implicit val ctagV = implicitly[Meta[V]].ctag
    implicit val ttagV = implicitly[Meta[V]].ttag
    override def ctag = implicitly[ClassTag[Triangle[V]]]
    override def ttag = implicitly[TypeTag[Triangle[V]]]
  }
  //@formatter:on
}
