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

import api._

import shapeless._

import scala.Function.const
import scala.collection.generic.CanBuild
import scala.language.higherKinds
import scala.reflect.ClassTag

/**
 * Codec (provides encoding and decoding) type-class for CSV records.
 * A CSV record consists of a fixed number of columns, encoded as an array of Strings.
 *
 * This codec is unsafe. Upon failure (mostly when decoding), an exception may be thrown.
 * A very common exception type is [[NumberFormatException]].
 *
 * Provided by default are codecs for Scala and Java primitives, enumerations, [[Option]],
 * [[Array]], [[Traversable]] collections, tuples and case classes.
 */
trait CSVConverter[T] {

  /** Returns the number of columns in a record for type `T`. */
  def size: Int

  /**
   * Decodes an instance of `T` from a CSV `record`.
   * Decoding is unsafe as it may throw an exception if parsing failed.
   * A very common exception type is [[NumberFormatException]].
   */
  def from(record: Array[String])(implicit csv: CSV): T

  /** Encodes an instance of `T` as a CSV record. */
  def to(value: T)(implicit csv: CSV): Array[String]
}

/** Factory methods and implicit instances of CSV converters. */
object CSVConverter {

  /** Summons an implicit codec for type `T` available in scope. */
  def apply[T](implicit converter: CSVConverter[T]): CSVConverter[T] =
    converter

  /** Creates a new codec for type `T` from pure conversion functions. */
  def make[T](length: Int)(fromF: Array[String] => T, toF: T => Array[String])
    : CSVConverter[T] = new CSVConverter[T] {
      def size = length
      def from(record: Array[String])(implicit csv: CSV) = fromF(record)
      def to(value: T)(implicit csv: CSV) = toF(value)
    }

  /**
   * Derives a new codec for type `B` from an existing codec for type `A`,
   * given an isomorphism between `A` and `B`.
   */
  def iso[A, B](fromF: A => B, toF: B => A)(implicit A: CSVConverter[A]): CSVConverter[B] =
    new CSVConverter[B] {
      def size = A.size
      def from(record: Array[String])(implicit csv: CSV) = fromF(A from record)
      def to(value: B)(implicit csv: CSV) = A to toF(value)
    }

  /** Creates a codec with a single column. */
  implicit def singleColumnCSVConverter[T](implicit T: CSVColumn[T]): CSVConverter[T] =
    new CSVConverter[T] {
      def size = 1
      def from(record: Array[String])(implicit csv: CSV) = T from record.head
      def to(value: T)(implicit csv: CSV) = Array(T to value)
    }

  /** An empty codec for [[HNil]]. */
  implicit val hNilCSVConverter: CSVConverter[HNil] =
    make(0)(const(HNil), const(Array.empty))

  /** Inductively generates a codec for an [[HList]], given codecs for its elements. */
  implicit def hConsCSVConverter[H, T <: HList](
    implicit H: Lazy[CSVConverter[H]], T: Lazy[CSVConverter[T]]
  ): CSVConverter[H :: T] = new CSVConverter[H :: T] {
    val size = H.value.size + T.value.size

    def from(record: Array[String])(implicit csv: CSV) = {
      val (h, t) = record splitAt H.value.size
      H.value.from(h) :: T.value.from(t)
    }

    def to(value: H :: T)(implicit csv: CSV) = {
      val h :: t = value
      H.value.to(h) ++ T.value.to(t)
    }
  }

  /** Generates a codec for a case class, given codecs for its fields. */
  implicit def genericCSVConverter[T, R <: HList](
    implicit gen: Generic.Aux[T, R], R: Lazy[CSVConverter[R]]
  ): CSVConverter[T] = new CSVConverter[T] {
    def size = R.value.size
    def from(record: Array[String])(implicit csv: CSV) = gen.from(R.value from record)
    def to(value: T)(implicit csv: CSV) = R.value.to(gen to value)
  }

  /**
   * Creates an [[Array]] codec from an element column.
   * In this way only flat arrays are supported.
   * A [[ClassTag]] is required to decode instances.
   */
  implicit def arrayCSVConverter[A](
    implicit A: CSVColumn[A], ctag: ClassTag[A]
  ): CSVConverter[Array[A]] = singleColumnCSVConverter(new CSVColumn[Array[A]] {
    def from(column: String)(implicit csv: CSV) = column split csv.delimitSeq map A.from
    def to(value: Array[A])(implicit csv: CSV) = value map A.to mkString csv.delimitSeq.toString
  })

  /**
   * Creates a codec for a [[Traversable]] collection from an element column.
   * In this way only flat collections are supported.
   */
  implicit def traversableCSVConverter[T[x] <: Traversable[x], A](
    implicit TA: CanBuild[A, T[A]], A: CSVColumn[A]
  ): CSVConverter[T[A]] = singleColumnCSVConverter(new CSVColumn[T[A]] {
    def from(column: String)(implicit csv: CSV) = column split csv.delimitSeq map A.from to TA
    def to(value: T[A])(implicit csv: CSV) = value map A.to mkString csv.delimitSeq.toString
  })

  /**
   * Creates a [[DataBag]] codec from an element column.
   * Note: data must be fetched before encoding.
   */
  implicit def dataBagCSVConverter[A: CSVColumn: Meta]: CSVConverter[DataBag[A]] =
    iso[Seq[A], DataBag[A]](DataBag(_), _.fetch())
}
