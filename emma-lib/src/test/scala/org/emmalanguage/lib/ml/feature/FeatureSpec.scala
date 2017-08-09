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

trait FeatureSpec extends lib.BaseLibSpec {

  val texts = Array(
    s"""
      |'Twas brillig, and the slithy toves
      |  Did gyre and gimble in the wabe;
      |All mimsy were the borogoves,
      |  And the mome raths outgrabe.
      """.stripMargin,
    s"""
      |"Beware the Jabberwock, my son!
      |  The jaws that bite, the claws that catch!
      |Beware the Jubjub bird, and shun
      |  The frumious Bandersnatch!"
      """.stripMargin
  )

  val tokenss = Array(
    Array(
      "Twas", "brillig", "and", "the", "slithy", "toves",
      "Did", "gyre", "and", "gimble", "in", "the", "wabe",
      "All", "mimsy", "were", "the", "borogoves",
      "And", "the", "mome", "raths", "outgrabe"),
    Array(
      "Beware", "the", "Jabberwock", "my", "son",
      "The", "jaws", "that", "bite", "the", "claws", "that", "catch",
      "Beware", "the", "Jubjub", "bird", "and", "shun",
      "The", "frumious", "Bandersnatch")
  )
}
