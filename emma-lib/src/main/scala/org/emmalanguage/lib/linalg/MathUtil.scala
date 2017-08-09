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
package lib.linalg

private[linalg] trait MathUtil {
  /**
   * Calculates 'x' modulo 'mod', takes to consideration sign of x,
   * i.e. if 'x' is negative, than 'x' % 'mod' is negative too so
   * function return (x % mod) + mod in that case.
   */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  /** Firscher-Yates shuffle. */
  def shuffle(n: Int)(r: util.RanHash): Array[Int] = {
    val xs = (1 to n).toArray
    for {
      i <- 0 to (n - 2)
    } {
      val j = r.nextInt(i + 1)
      val t = xs(i)
      xs(i) = xs(j)
      xs(j) = t
    }
    xs
  }
}
