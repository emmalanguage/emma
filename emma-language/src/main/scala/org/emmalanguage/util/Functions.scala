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
package util

/** Utilities for [[scala.Function]]s. */
object Functions {

  /** Syntax sugar for partial functions. */
  type =?>[-A, +R] = PartialFunction[A, R]

  /** Applies a partial function to the `args` or else return `default`. */
  def complete[A, R](pf: A =?> R)(args: A)(default: => R): R =
    pf.applyOrElse(args, (_: A) => default)

  /** Converts `f` to a partial function that is always defined. */
  def partial[A, R](f: A => R): A =?> R = PartialFunction(f)

  /** Creates a partial function that always returns `value`. */
  def partialConst[A, R](value: R): A =?> R = partial(Function.const(value))

  /** Composes the partial function `pf` with a regular function `f`. */
  def compose[A, B, C](pf: B =?> C)(f: A => B): A =?> C = {
    case a if pf.isDefinedAt(f(a)) => pf(f(a))
  }
}
