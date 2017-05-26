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
package compiler.lang.backend

/** Binding context type. */
class BCtx private(val value: Int) extends AnyVal {
  def &(that: BCtx): BCtx = new BCtx(this.value & that.value)

  def |(that: BCtx): BCtx = new BCtx(this.value | that.value)
}

object BCtx {
  val Unknown = new BCtx(0)
  val Driver = new BCtx(1)
  val Engine = new BCtx(2)
  val Ambiguous = new BCtx(3)
}
