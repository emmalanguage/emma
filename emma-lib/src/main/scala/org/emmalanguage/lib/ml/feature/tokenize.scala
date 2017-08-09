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
object tokenize {
  def gaps(
    pattern: String = "\\W+",
    minTokenLength: Int = 1,
    toLowercase: Boolean = true
  )(s: String): Array[String] = {
    val re = pattern.r
    val string =
      if (toLowercase) s.toLowerCase()
      else s
    for {
      f <- re.split(string)
      if f.length >= minTokenLength
    } yield f
  }

  def words(
    pattern: String = "\\w+",
    minTokenLength: Int = 1,
    toLowercase: Boolean = true
  )(s: String): Array[String] = {
    val re = pattern.r
    val string =
      if (toLowercase) s.toLowerCase()
      else s
    for {
      f <- re.findAllIn(string).toArray
      if f.length >= minTokenLength
    } yield f
  }
}
