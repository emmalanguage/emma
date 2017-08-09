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
package lib.ml.feature

import api._

@emma.lib
object nGrams {
  def apply(n: Int)(tokens: Array[String]): Array[String] = {
    val L = tokens.length - n + 1
    if (L < 0) Array.empty[String]
    else {
      val ngrams = Array.ofDim[String](L)
      var i = 0
      while (i < L) {
        var s = tokens(i)
        val M = i + n
        var j = i + 1
        while (j < M) {
          s = s concat " "
          s = s concat tokens(j)
          j += 1
        }
        ngrams(i) = s
        i += 1
      }
      ngrams
    }
  }
}
