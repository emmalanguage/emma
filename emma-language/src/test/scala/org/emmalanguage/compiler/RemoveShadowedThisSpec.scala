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
package compiler

import test.TestMacros

import org.scalatest.FreeSpec
import org.scalatest.Matchers

class RemoveShadowedThisSpec extends FreeSpec with Matchers {

  class A {
    outer =>
    val a = 42
    val b = a

    class B {
      middle =>
      val a = 0

      class A {
        inner =>
        val a = -42

        val v = TestMacros.removeShadowedThis(b == 42)
        val x = TestMacros.removeShadowedThis(B.this.a == 0)
        val y = TestMacros.removeShadowedThis(outer.a == 42 && middle.a == 0 && inner.a == -42)
      }

    }

  }

  val A1 = new A
  val B1 = new A1.B
  val A2 = new B1.A

  "should remove shadowed `this.` prefix " in {
    A2.v shouldBe true
  }

  "should not remove non-shadowed `this.` prefix" in {
    A2.x shouldBe true
  }

  "should not change access via self-objects" ignore {
    A2.y shouldBe true
  }

}
