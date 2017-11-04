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
package compiler.lang.source

import compiler.BaseCompilerSpec

/** A spec for the `Core.foreach2loop` transformation. */
//noinspection ScalaUnusedSymbol
class PatternMatchingSpec extends BaseCompilerSpec {

  import PatternMatchingSpec._
  import compiler._
  import u.reify

  val destructPatternMatchesPipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true, withPre = false)(
      fixSymbolTypes,
      unQualifyStatics,
      normalizeStatements,
      PatternMatching.destruct.timed
    ).compose(_.tree)

  val noopPipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true, withPre = false)(
      fixSymbolTypes,
      unQualifyStatics,
      normalizeStatements
    ).compose(_.tree)

  "pattern match" - {
    "with ref lhs" in {

      val dogs = (Dog("Max"), Dog("Gidget"))

      val act = destructPatternMatchesPipeline(reify {
        val nameFst = dogs match {
          case pair@(Dog(name), _) => name
        }

        val nameSnd = dogs match {
          case (_, snd@Dog(name)) => name
        }

        nameFst == nameSnd
      })

      val exp = idPipeline(reify {
        val nameFst = {
          val pair = dogs
          val name = pair._1.name
          name
        }

        val nameSnd = {
          val snd = dogs._2
          val name = snd.name
          name
        }

        nameFst == nameSnd
      })

      act shouldBe alphaEqTo (exp)
    }

    "with type-ascribed lhs" in {

      val dogs = (Dog("Max"), Dog("Gidget"))

      val act = destructPatternMatchesPipeline(reify {
        val nameFst = (dogs: (Dog, Dog)@unchecked) match {
          case pair@(Dog(name), _) => name
        }

        val nameSnd = dogs match {
          case (_, snd@Dog(name)) => name
        }

        nameFst == nameSnd
      })

      val exp = idPipeline(reify {
        val nameFst = {
          val pair = dogs
          val name = pair._1.name
          name
        }

        val nameSnd = {
          val snd = dogs._2
          val name = snd.name
          name
        }

        nameFst == nameSnd
      })

      act shouldBe alphaEqTo (exp)
    }

    "with complex lhs" in {

      val act = destructPatternMatchesPipeline(reify {
        val nameFst = (Dog("Max"), Dog("Gidget")) match {
          case pair@(Dog(name), _) => name
        }

        val nameSnd = (Dog("Max"), Dog("Gidget")) match {
          case (_, snd@Dog(name)) => name
        }

        nameFst == nameSnd
      })

      val exp = idPipeline(reify {
        val nameFst = {
          val x$1 = (Dog("Max"), Dog("Gidget"))
          val pair = x$1
          val name = pair._1.name
          name
        }

        val nameSnd = {
          val x$2 = (Dog("Max"), Dog("Gidget"))
          val snd = x$2._2
          val name = snd.name
          name
        }

        nameFst == nameSnd
      })

      act shouldBe alphaEqTo (exp)
    }
  }
}

object PatternMatchingSpec {

  sealed trait Animal {
    def name: String
  }

  case class Cat(name: String) extends Animal
  case class Dog(name: String) extends Animal
  case class Hawk(name: String) extends Animal
}
