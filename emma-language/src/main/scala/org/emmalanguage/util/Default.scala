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

import scala.collection.generic.CanBuild
import scala.language.higherKinds
import scala.reflect.ClassTag

/** Type class for providing a default value for type [[A]]. */
trait Default[A] {
  def value: A
  def nullable: Boolean
  def empty(x: A): Boolean
}

/** [[Default]] instances. */
object Default extends DefaultLowPriority {
  def apply[A: Default]: Default[A] = implicitly

  implicit val string: Default[String] =
    new Default[String] {
      def value = ""
      def nullable = true
      def empty(x: String) = x == null || x.isEmpty
    }

  implicit def optional[A]: Default[Option[A]] =
    new Default[Option[A]] {
      def value = None
      def nullable = true
      def empty(x: Option[A]) = x == null || x.isEmpty || x.get == null
    }

  implicit def array[A: ClassTag]: Default[Array[A]] =
    new Default[Array[A]] {
      def value = Array.empty
      def nullable = true
      def empty(x: Array[A]) = x == null || x.isEmpty
    }

  implicit def traversable[T[x] <: Traversable[x], E](
    implicit cbf: CanBuild[E, T[E]]
  ): Default[T[E]] = new Default[T[E]] {
    def value = cbf().result()
    def nullable = true
    def empty(x: T[E]) = x == null || x.isEmpty
  }

  implicit def map[M[k, v] <: Map[k, v], K, V](
    implicit cbf: CanBuild[(K, V), M[K, V]]
  ): Default[M[K, V]] = new Default[M[K, V]] {
    def value = cbf().result()
    def nullable = true
    def empty(x: M[K, V]) = x == null || x.isEmpty
  }
}

/** Low priority [[Default]] instances. */
trait DefaultLowPriority {

  implicit def anyVal[A <: AnyVal]: Default[A] =
    new Default[A] {
      def value = null.asInstanceOf[A]
      def nullable = false
      def empty(x: A) = false
    }

  implicit def anyRef[A >: Null <: AnyRef]: Default[A] =
    new Default[A] {
      def value = null
      def nullable = true
      def empty(x: A) = x == null
    }
}
