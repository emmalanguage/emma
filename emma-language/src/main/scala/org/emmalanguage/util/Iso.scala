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

import api._

import java.{lang => jl}
import java.{math => jm}
import java.{util => ju}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag

/** Type class proving an isomorphism between types `A` and `B`. */
trait Iso[A, B] extends Serializable {
  def from(a: A): B
  def to(b: B): A

  /** Swaps the type arguments of this isomorphism. */
  def swap: B <=> A = Iso.make(to, from)

  /** Composes with `B <-> C` isomorphism to provide `A <-> C` isomorphism. */
  def compose[C](implicit that: B <=> C): A <=> C =
    Iso.make(a => that.from(from(a)), c => to(that.to(c)))
}

/** Factory methods and instances of isomorphism. */
object Iso {

  /** Summons an implicit isomorphism instance available in scope. */
  def apply[A, B](implicit iso: A <=> B): A <=> B = iso

  /**
   * Creates a new isomorphism from pure view functions.
   * Handles Java boxed primitives and collections
   * (the latter requires `import scala.collection.JavaConversions._`).
   */
  implicit def make[A, B](implicit fromF: A => B, toF: B => A): A <=> B =
    new Iso[A, B] {
      def from(a: A) = fromF(a)
      def to(b: B) = toF(b)
    }

  implicit val stringIsoBigInt: String <=> BigInt =
    make(BigInt.apply, _.toString)

  implicit val stringIsoBigDecimal: String <=> BigDecimal =
    make(BigDecimal.apply, _.toString)

  implicit val bigIntIsoJBigInteger: BigInt <=> jm.BigInteger =
    make(_.bigInteger, BigInt.apply)

  implicit val bigDecimalIsoJBigDecimal: BigDecimal <=> jm.BigDecimal =
    make(_.bigDecimal, BigDecimal.apply)

  implicit val longIsoDate: Long <=> ju.Date =
    make(new ju.Date(_), _.getTime)

  implicit def bufferIsoArray[A: ClassTag]: mutable.Buffer[A] <=> Array[A] =
    make(_.toArray, _.toBuffer)

  implicit def seqIsoDataBag[A: Meta]: Seq[A] <=> DataBag[A] =
    make(DataBag(_), _.collect())

  implicit def intIsoEnum[E <: Enumeration](implicit enum: E): Int <=> enum.Value =
    make(enum(_), _.id)

  implicit def intIsoJEnum[E <: jl.Enum[E]](implicit E: ClassTag[E]): Int <=> E =
    make(ordinal => {
      val enum = E.runtimeClass.asInstanceOf[Class[E]]
      ju.EnumSet.allOf(enum).find(_.ordinal == ordinal).get
    }, _.ordinal)

  def stringIsoEnum[E <: Enumeration](implicit enum: E): String <=> enum.Value =
    make(name => enum.withName(name), _.toString)

  def stringIsoJEnum[E <: jl.Enum[E]](implicit E: ClassTag[E]): String <=> E =
    make(jl.Enum.valueOf(E.runtimeClass.asInstanceOf[Class[E]], _), _.name)
}
