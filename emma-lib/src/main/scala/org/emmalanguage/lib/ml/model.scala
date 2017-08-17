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
package lib.ml

import api._
import lib.linalg._

/** Point with identity and a dense vector position. */
case class DPoint[ID](@emma.pk id: ID, pos: DVector) {
  def this(id: ID, pos: Vector) = this(id, pos match {
    case pos: DVector => pos
    case pos: SVector => pos.toDense
  })
}

/** Point with identity and a sparse vector position. */
case class SPoint[ID](@emma.pk id: ID, pos: SVector) {
  def this(id: ID, pos: Vector) = this(id, pos match {
    case pos: DVector => pos.toSparse
    case pos: SVector => pos
  })
}

/** Point with identity, a dense vector position, and a label. */
case class LDPoint[ID, L](@emma.pk id: ID, pos: DVector, label: L) {
  def this(id: ID, pos: Vector, label: L) = this(id, pos match {
    case pos: DVector => pos
    case pos: SVector => pos.toDense
  }, label)
}

/** Point with identity, a dense vector position, and a label. */
case class LSPoint[ID, L](@emma.pk id: ID, pos: SVector, label: L) {
  def this(id: ID, pos: Vector, label: L) = this(id, pos match {
    case pos: DVector => pos.toSparse
    case pos: SVector => pos
  }, label)
}

/** Point with identity, weight, dense vector position, and label */
case class WLDPoint[ID, L](@emma.pk id: ID, w: Double, pos: DVector, label: L) {
  def this(id: ID, w: Double, pos: Vector, label: L) = this(id, w, pos match {
    case pos: DVector => pos
    case pos: SVector => pos.toDense
  }, label)
}

/** Point with identity, weight, sparse vector position, and label */
case class WLSPoint[ID, L](@emma.pk id: ID, w: Double, pos: SVector, label: L) {
  def this(id: ID, w: Double, pos: Vector, label: L) = this(id, w, pos match {
    case pos: DVector => pos.toSparse
    case pos: SVector => pos
  }, label)
}

/** Features point. */
case class FPoint[ID, F](@emma.pk id: ID, features: F)

case class LinearModel(weights: DVector, lossHistory: Array[Double] = Array.empty) {
  def this(weights: Vector, lossHistory: Array[Double]) = this(weights match {
    case weights: DVector => weights
    case weights: SVector => weights.toDense
  }, lossHistory)
}
