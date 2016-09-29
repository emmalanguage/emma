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

import io.csv.{CSVConverter, CSVConverterMacro}
import eu.stratosphere.emma.macros.Folds

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Package object for the Emma API. Contains default methods and definitions. */
package object api {

  import scala.language.experimental.macros

  // -----------------------------------------------------
  // types supported by Emma
  // -----------------------------------------------------

  //@formatter:off
  trait Meta[T] extends Serializable {
    def ctag: ClassTag[T]
    def ttag: TypeTag[T]
  }

  implicit def meta[T : ClassTag : TypeTag] = new Meta[T] {
    override def ctag = implicitly[ClassTag[T]]
    override def ttag = implicitly[TypeTag[T]]
  }
  //@formatter:on

  implicit def ttagForType[T: Meta]: TypeTag[T] =
    implicitly[Meta[T]].ttag

  implicit def ctagForType[T: Meta]: ClassTag[T] =
    implicitly[Meta[T]].ctag

  // -----------------------------------------------------
  // limits
  // -----------------------------------------------------

  trait Limits[T] {
    val min: T
    val max: T
  }

  implicit val ByteLimits = new Limits[Byte] {
    val min = Byte.MinValue
    val max = Byte.MaxValue
  }

  implicit val IntLimits = new Limits[Int] {
    val min = Int.MinValue
    val max = Int.MaxValue
  }

  implicit val LongLimits = new Limits[Long] {
    val min = Long.MinValue
    val max = Long.MaxValue
  }

  implicit val CharLimits = new Limits[Char] {
    val min = Char.MinValue
    val max = Char.MaxValue
  }

  implicit val FloatLimits = new Limits[Float] {
    val min = Float.MinValue
    val max = Float.MaxValue
  }

  implicit val DoubleLimits = new Limits[Double] {
    val min = Double.MinValue
    val max = Double.MaxValue
  }

  // -----------------------------------------------------
  // Converters
  // -----------------------------------------------------

  /**
   * Extend the [[DataBag]] type with methods from [[Folds]] via the "pimp my library" pattern.
   * This is a value class, which means that in most cases, the allocation of an instance can be
   * avoided when using the defined methods.
   *
   * @param self the actual [[DataBag]] instance
   * @tparam A the type of elements to fold over
   */
  implicit final class DataBagFolds[A] private[api](val self: DataBag[A])
    extends AnyVal with Folds[A]

  implicit def materializeCSVConverter[T]: CSVConverter[T] =
    macro CSVConverterMacro.materialize[T]

  def comparing[A](lt: (A, A) => Boolean): Ordering[A] =
    Ordering.fromLessThan(lt)
}
