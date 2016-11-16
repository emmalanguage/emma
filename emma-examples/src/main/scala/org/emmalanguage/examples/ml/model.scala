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
package examples.ml

import api._

import breeze.linalg.Vector

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Machine learning model objects. */
object model {

  case class Point[ID: Meta](@emma.pk id: ID, vector: Vector[Double])

  case class LVector[L: Meta](label: L, vector: Vector[Double])

  //@formatter:off
  implicit def pointMeta[ID: Meta]: Meta[Point[ID]] = new Meta[Point[ID]] {
    implicit def ctagID = implicitly[Meta[ID]].ctag
    implicit def ttagID = implicitly[Meta[ID]].ttag
    override def ctag = implicitly[ClassTag[Point[ID]]]
    override def ttag = implicitly[TypeTag[Point[ID]]]
  }

  implicit def edgeMeta[L: Meta]: Meta[LVector[L]] = new Meta[LVector[L]] {
    implicit def ctagL = implicitly[Meta[L]].ctag
    implicit def ttagL = implicitly[Meta[L]].ttag
    override def ctag = implicitly[ClassTag[LVector[L]]]
    override def ttag = implicitly[TypeTag[LVector[L]]]
  }
  //@formatter:on
}
