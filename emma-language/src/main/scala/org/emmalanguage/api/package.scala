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

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/** Package object for the Emma API. Contains default methods and definitions. */
package object api {

  // -----------------------------------------------------
  // types supported by Emma
  // -----------------------------------------------------

  //@formatter:off
  trait Meta[T] extends Serializable {
    def ctag: ClassTag[T]
    def ttag: TypeTag[T]
  }

  object Meta {
    type Tag[T] = TypeTag[T]

    implicit def apply[T: Tag]: Meta[T] = new Meta[T] {
      lazy val ctag = ClassTag[T](ttag.mirror.runtimeClass(ttag.tpe))
      def ttag = implicitly[TypeTag[T]]
    }

    object Projections {
      implicit def ttagFor[T](implicit meta: Meta[T]): TypeTag[T]  = meta.ttag
      implicit def ctagFor[T](implicit meta: Meta[T]): ClassTag[T] = meta.ctag
    }
  }
  //@formatter:on
}
