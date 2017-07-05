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
package compiler.ir

import scala.language.higherKinds

/** Dummy IR nodes that model comprehension syntax in the Emma IR. */
object ComprehensionSyntax {

  def generator[A, M[_]](in: M[A]): A = ???

  def comprehension[A, M[_]](block: A): M[A] = ???

  def guard(expr: Boolean): Nothing = ???

  def head[A](expr: A): A = ???
}
