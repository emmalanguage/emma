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

import java.io.Serializable

/**
 * c.f. "Press, William H. Numerical recipes 3rd edition: The art of scientific computing. Cambridge
 * university press, 2007.", pp. 352
 */
class RanHash private(val seed: Long, var currPos: Long = 0) extends Serializable {

  def at(pos: Long): RanHash = {
    currPos = pos
    this
  }

  def skip(pos: Long): RanHash = {
    currPos += pos
    this
  }

  def fork: RanHash =
    new RanHash(seed)

  def toStream: Stream[Double] = {
    val rand = this.fork
    Stream.continually(rand.next())
  }

  def next(): Double = {
    var x = seed + currPos
    x = 3935559000370003845L * x + 2691343689449507681L
    x = x ^ (x >> 21)
    x = x ^ (x << 37)
    x = x ^ (x >> 4)
    x = 4768777513237032717L * x
    x = x ^ (x << 20)
    x = x ^ (x >> 41)
    x = x ^ (x << 5)
    currPos += 1
    x * RanHash.D_2_POW_NEG_64 + 0.5
  }

  def nextInt(k: Int): Int =
    Math.floor(next * k).toInt

  def nextLong(k: Long): Long =
    Math.floor(next * k).toLong
}

object RanHash {
  private val D_2_POW_NEG_64 = 5.4210108624275221700e-20

  def apply(seed: Long, substream: Int = 0): RanHash =
    new RanHash(seed + substream * (2L << 54))
}
