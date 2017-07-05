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
package io.csv

import util._

import java.{lang => jl}

import scala.Function.const
import scala.reflect.ClassTag

/**
 * Codec (provides encoding and decoding) type-class for CSV columns.
 * A CSV column is an opaque value within a CSV record, encoded as a String.
 * E.g. a CSV record of only one column would need no column delimiter.
 *
 * This codec is unsafe. Upon failure (mostly when decoding), an exception may be thrown.
 * A very common exception type is [[NumberFormatException]].
 *
 * Usually CSV column codecs are provided for types that can be encoded as and decoded from a
 * String, but cannot be broken down to a fixed number of fields. Otherwise [[CSVConverter]] is
 * better suited. Instances provided by default are Scala and Java primitives, enumerations and
 * [[Option]].
 */
trait CSVColumn[T] extends Serializable {

  /**
   * Decodes an instance of `T` from a String.
   * Decoding is unsafe as it may throw an exception if parsing failed.
   * A very common exception type is [[NumberFormatException]].
   */
  def from(column: String)(implicit csv: CSV): T

  /**
   * Encodes an instance of `T` as a String.
   * In many cases delegating to `value.toString` is sufficient.
   */
  def to(value: T)(implicit csv: CSV): String
}

/** Factory methods and implicit instances of CSV column codecs. */
object CSVColumn extends IsoCSVColumns {

  /** Summons an implicit codec for type `T` available in scope. */
  def apply[T](implicit column: CSVColumn[T]): CSVColumn[T] = column

  /** Creates a new codec for type `T` from pure conversion functions. */
  def make[T](fromF: String => T, toF: T => String = (_: T).toString)
    : CSVColumn[T] = new CSVColumn[T] {
      def from(column: String)(implicit csv: CSV) = fromF(column)
      def to(value: T)(implicit csv: CSV) = toF(value)
    }

  // Scala primitives
  implicit val stringCSVColumn:     CSVColumn[String]     = make(identity)
  implicit val charCSVColumn:       CSVColumn[Char]       = make(_.head)
  implicit val booleanCSVColumn:    CSVColumn[Boolean]    = make(_.toBoolean)
  implicit val byteCSVColumn:       CSVColumn[Byte]       = make(_.toByte)
  implicit val shortCSVColumn:      CSVColumn[Short]      = make(_.toShort)
  implicit val intCSVColumn:        CSVColumn[Int]        = make(_.toInt)
  implicit val longCSVColumn:       CSVColumn[Long]       = make(_.toLong)
  implicit val floatCSVColumn:      CSVColumn[Float]      = make(_.toFloat)
  implicit val doubleCSVColumn:     CSVColumn[Double]     = make(_.toDouble)
  implicit val unitCSVColumn:       CSVColumn[Unit]       = make(const(()), const(""))

  /**
   * Creates a Scala [[Enumeration]] codec.
   * Since enumerations are usually defined as objects,
   * the best way to provide an implicit instance is via
   * `implicit object MyEnum extends Enumeration`.
   */
  implicit def enumCSVColumn[E <: Enumeration](implicit enum: E): CSVColumn[enum.Value] =
    iso(Iso.stringIsoEnum[enum.type](enum), stringCSVColumn)

  /**
   * Creates a Java [[Enum]] codec.
   * A [[ClassTag]] is required to decode instances.
   */
  implicit def jEnumCSVColumn[E <: jl.Enum[E]](implicit E: ClassTag[E]): CSVColumn[E] =
    iso(Iso.stringIsoJEnum, stringCSVColumn)

  /** Creates an optional codec from an existing codec. */
  implicit def optionCSVColumn[T](implicit T: CSVColumn[T])
    : CSVColumn[Option[T]] = new CSVColumn[Option[T]] {
      def from(column: String)(implicit csv: CSV) =
        if (csv.nullValue == column) None else Option(T from column)

      def to(value: Option[T])(implicit csv: CSV) =
        value.fold(csv.nullValue)(T.to)
    }
}

// Lowest priority
trait IsoCSVColumns {

  /**
   * Derives a new codec for type `B` from an existing codec for type `A`,
   * given an (implicit) isomorphism between `A` and `B`.
   *
   * Handles Java primitives.
   */
  implicit def iso[A, B](implicit iso: A <=> B, underlying: CSVColumn[A])
    : CSVColumn[B] = new CSVColumn[B] {
      def from(column: String)(implicit csv: CSV) = iso.from(underlying.from(column))
      def to(value: B)(implicit csv: CSV) = underlying.to(iso.to(value))
    }
}
