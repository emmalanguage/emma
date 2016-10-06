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
package eu.stratosphere
package emma.macros

import scala.language.dynamics
import scala.language.implicitConversions
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
 * Adapted from
 * https://github.com/retronym/macrocosm/blob/master/src/main/scala/com/github/retronym/macrocosm/Macrocosm.scala
 */
object BinaryLiterals {

  implicit def from(sc: StringContext): Binary =
    new Binary(sc)

  class Binary(sc: StringContext) {
    // This is how a non-macro version would be implemented:
    // def b() = {
    //   val lit = sc.parts.mkString
    //   parseBin(lit).getOrElse(sys.error(s"Invalid binary literal: $lit"))
    // }

    /**
     * Binary literal integer.
     *
     * {{{
     * scala> b"101010"
     * res0: Int = 42
     * }}}
     */
    def b(): Int = macro binary
  }

  def binary(c: blackbox.Context)() = {
    import c.universe._

    def parseBin(lit: String): Int = {
      var i = lit.length - 1
      var sum = 0
      var ord = 1
      while (i >= 0) {
        lit.charAt(i) match {
          case '1' =>
            sum += ord
            ord *= 2
          case '0' =>
            ord *= 2
          case ' ' =>
          case _ =>
            val pos = c.enclosingPosition
            val err = s"Invalid binary literal: $lit"
            c.abort(pos, err)
        }

        i -= 1
      }

      sum
    }

    c.prefix.tree match {
      // e.g: `emma.macros.BinaryLiterals.from(scala.StringContext.apply("1111"))`
      case q"$_(scala.StringContext.apply(${lit: String}))" =>
        q"${parseBin(lit)}"
      case tree =>
        val pos = c.enclosingPosition
        val err = s"Unexpected tree: ${showCode(tree)}"
        c.abort(pos, err)
    }
  }
}
