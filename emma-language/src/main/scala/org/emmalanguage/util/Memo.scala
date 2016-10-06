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

import scala.collection.mutable

/**
 * Memoizing different types of functions.
 *
 * Calling a memoized function with a set of arguments for the first time will save the result in a
 * cache - a [[mutable.Map]], possibly user-provided. Calling it with the same set of arguments
 * subsequently will not perform the computation, but retrieve the cached value instead.
 *
 * This is useful for computations that are expensive and are performed many times with the same
 * arguments.
 *
 * However, functions need to be referentially transparent (i.e. have no side effects) to preserve
 * correctness.
 */
object Memo {

  /** Memoize a simple referentially transparent function */
  def apply[A, R](f: A => R,
    cache: mutable.Map[A, R] = mutable.Map.empty[A, R]): A => R = {

    arg => cache.getOrElseUpdate(arg, f(arg))
  }

  /**
   * Memoize a recursive function. Recursive invocations will also go through the cache, therefore
   * the function must accept it's cached variant as argument.
   *
   * Memoization of recursive functions is equivalent to dynamic programming.
   *
   * WARNING: Not stack safe.
   */
  def recur[A, R](f: (A => R) => A => R,
    cache: mutable.Map[A, R] = mutable.Map.empty[A, R]): A => R = {

    lazy val recur: A => R = apply[A, R](f(recur)(_), cache)
    recur
  }

  /** Memoize a commutative function (the order of arguments doesn't matter). */
  def commutative[A, R](f: (A, A) => R,
    cache: mutable.Map[(A, A), R] = mutable.Map.empty[(A, A), R]): (A, A) => R = {
      case (x, y) => cache.getOrElse(y -> x, cache.getOrElseUpdate(x -> y, f(x, y)))
    }
}
