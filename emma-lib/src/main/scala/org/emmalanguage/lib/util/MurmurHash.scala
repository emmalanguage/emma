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
package lib.util

import org.scalactic.Requirements

/** Ported from the spark-ml library. */
class MurmurHash(seed: Int) {
  def hashInt(input: Int) =
    MurmurHash.hashInt(input, seed)

  def hashUnsafeWords(base: AnyRef, offset: Long, lengthInBytes: Int): Int =
    MurmurHash.hashUnsafeWords(base, offset, lengthInBytes, seed)

  def hashLong(input: Long): Int =
    MurmurHash.hashLong(input, seed)

  override def toString: String =
    s"${getClass.getSimpleName}(seed=$seed)"
}

object MurmurHash extends Requirements {
  private[lib] val C1 = 0xcc9e2d51
  private[lib] val C2 = 0x1b873593

  def hashInt(input: Int, seed: Int) = {
    val k1 = mixK1(input)
    val h1 = mixH1(seed, k1)
    fmix(h1, 4)
  }

  def hashUnsafeWords(base: AnyRef, offset: Long, lengthInBytes: Int, seed: Int) = {
    require(lengthInBytes % 8 == 0, ": lengthInBytes must be a multiple of 8 (word-aligned)")
    val h1 = hashBytesByInt(base, offset, lengthInBytes, seed)
    fmix(h1, lengthInBytes)
  }

  def hashUnsafeBytes(base: AnyRef, offset: Long, lengthInBytes: Int, seed: Int) = {
    require(lengthInBytes >= 0, "lengthInBytes cannot be negative")
    val lengthAligned = lengthInBytes - lengthInBytes % 4
    var h1 = hashBytesByInt(base, offset, lengthAligned, seed)
    var i = lengthAligned
    while(i < lengthInBytes) {
      val halfWord = Platform.getByte(base, offset + i)
      val k1 = mixK1(halfWord)
      h1 = mixH1(h1, k1)
      i += 1
    }
    fmix(h1, lengthInBytes)
  }

  def hashBytesByInt(base: AnyRef, offset: Long, lengthInBytes: Int, seed: Int) = {
    require(lengthInBytes % 4 == 0)
    var h1 = seed
    var i = 0
    while(i < lengthInBytes) {
      val halfWord = Platform.getInt(base, offset + i)
      val k1 = mixK1(halfWord)
      h1 = mixH1(h1, k1)
      i += 4
    }
    h1
  }

  def hashLong(input: Long, seed: Int) = {
    val low = input.asInstanceOf[Int]
    val high = (input >>> 32).asInstanceOf[Int]
    var k1 = mixK1(low);
    var h1 = mixH1(seed, k1);
    k1 = mixK1(high);
    h1 = mixH1(h1, k1);
    fmix(h1, 8);
  }

  private[lib] def mixK1(k1: Int): Int =
    Integer.rotateLeft(k1 * C1, 15) * C2

  private[lib] def mixH1(h1: Int, k1: Int): Int =
    Integer.rotateLeft(h1 ^ k1, 13) * 5 + 0xe6546b64

  private[lib] def fmix(h1: Int, length: Int): Int = {
    var r = h1 ^ length
    r ^= h1 >>> 16
    r *= 0x85ebca6b
    r ^= r >>> 13
    r *= 0xc2b2ae35
    r ^= r >>> 16
    r
  }
}
