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
package examples.text

import api._

@emma.lib
object WordCount {

  def apply(docs: DataBag[String]): DataBag[(String, Long)] = {
    val words = for {
      line <- docs
      word <- DataBag[String](line.toLowerCase.split("\\W+"))
      if word != ""
    } yield word

    // group the words by their identity and count the occurrence of each word
    val counts = for {
      group <- words.groupBy(identity)
    } yield (group.key, group.values.size)

    counts
  }
}
