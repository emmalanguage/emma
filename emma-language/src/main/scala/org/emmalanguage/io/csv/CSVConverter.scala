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
import util._

import shapeless._

import scala.collection.breakOut
import scala.collection.generic.CanBuild
import scala.language.higherKinds
import scala.reflect.ClassTag

/**
 * Codec (provides encoding and decoding) type-class for CSV records.
 * A CSV record consists of a fixed number of columns, encoded as an array of Strings.
 *
 * This codec is unsafe. Upon failure (mostly when reading), an exception may be thrown.
 * A very common exception type is [[NumberFormatException]].
 *
 * Provided by default are codecs for Scala and Java primitives, enumerations, [[Option]],
 * [[Array]], [[Traversable]] collections, tuples and case classes.
 */
trait CSVConverter[T] extends Serializable {

  /** Returns the number of columns in a record for type `T`. */
  def size: Int

  /**
   * Reads an instance of `T` from a CSV `record` at position `i`.
   * Decoding is unsafe as it may throw an exception if reading failed.
   * A very common exception type is [[NumberFormatException]].
   */
  def read(record: Array[String], i: Int)(implicit csv: CSV): T

  /** Writes an instance of `T` in a CSV `record` at position `i`. */
  def write(value: T, record: Array[String], i: Int)(implicit csv: CSV): Unit
}

/** Factory methods and implicit instances of CSV converters. */
object CSVConverter extends Serializable {

  /** Summons an implicit codec for type `T` available in scope. */
  def apply[T](implicit converter: CSVConverter[T]): CSVConverter[T] =
    converter

  /**
   * Derives a new codec for type `B` from an existing codec for type `A`,
   * given an (implicit) isomorphism between `A` and `B`.
   */
  def iso[A, B](implicit iso: A <=> B, underlying: CSVConverter[A])
    : CSVConverter[B] = new CSVConverter[B] {
      def size = underlying.size
      def read(record: Array[String], i: Int)(implicit csv: CSV) =
        iso.from(underlying.read(record, i))
      def write(value: B, record: Array[String], i: Int)(implicit csv: CSV) =
        underlying.write(iso.to(value), record, i)
    }

  /** Creates a codec with a single column. */
  implicit def singleColumnCSVConverter[T](implicit T: CSVColumn[T]): CSVConverter[T] =
    new CSVConverter[T] {
      def size = 1
      def read(record: Array[String], i: Int)(implicit csv: CSV) =
        T.from(record(i))
      def write(value: T, record: Array[String], i: Int)(implicit csv: CSV) =
        record(i) = T.to(value)
    }

  /** An empty codec for [[HNil]]. */
  implicit val hNilCSVConverter: CSVConverter[HNil] =
    new CSVConverter[HNil] {
      def size = 0
      def read(record: Array[String], i: Int)(implicit csv: CSV) = HNil
      def write(value: HNil, record: Array[String], i: Int)(implicit csv: CSV) = ()
    }

  /** Inductively generates a codec for an [[HList]], given codecs for its elements. */
  implicit def hConsCSVConverter[H, T <: HList](
    implicit H: Lazy[CSVConverter[H]], T: Lazy[CSVConverter[T]]
  ): CSVConverter[H :: T] = new CSVConverter[H :: T] {
    private def h = H.value
    private def t = T.value
    val size = h.size + t.size
    def read(record: Array[String], i: Int)(implicit csv: CSV) =
      h.read(record, i) :: t.read(record, i + h.size)
    def write(value: H :: T, record: Array[String], i: Int)(implicit csv: CSV) = {
      h.write(value.head, record, i)
      t.write(value.tail, record, i + h.size)
    }
  }

  /** Generates a codec for a case class, given codecs for its fields. */
  implicit def genericCSVConverter[T, R <: HList](
    implicit gen: Generic.Aux[T, R], R: Lazy[CSVConverter[R]]
  ): CSVConverter[T] = new CSVConverter[T] {
    private def r = R.value
    def size = r.size
    def read(record: Array[String], i: Int)(implicit csv: CSV) =
      gen.from(r.read(record, i))
    def write(value: T, record: Array[String], i: Int)(implicit csv: CSV) =
      r.write(gen.to(value), record, i)
  }

  /**
   * Creates an [[Array]] codec from an element column.
   * In this way only flat arrays are supported.
   * A [[ClassTag]] is required to decode instances.
   */
  implicit def arrayCSVConverter[A](
    implicit A: CSVColumn[A], ctag: ClassTag[A]
  ): CSVConverter[Array[A]] = singleColumnCSVConverter(
    new CSVColumn[Array[A]] {
      def from(column: String)(implicit csv: CSV) =
        column.split(csv.delimitSeq).map(A.from)
      def to(value: Array[A])(implicit csv: CSV) =
        value.iterator.map(A.to).mkString(csv.delimitSeq.toString)
    })

  /**
   * Creates a codec for a [[Traversable]] collection from an element column.
   * In this way only flat collections are supported.
   */
  implicit def traversableCSVConverter[T[x] <: Traversable[x], A](
    implicit TA: CanBuild[A, T[A]], A: CSVColumn[A]
  ): CSVConverter[T[A]] = singleColumnCSVConverter(
    new CSVColumn[T[A]] {
      def from(column: String)(implicit csv: CSV) =
        column.split(csv.delimitSeq).map(A.from)(breakOut)
      def to(value: T[A])(implicit csv: CSV) =
        value.toIterator.map(A.to).mkString(csv.delimitSeq.toString)
    })

  /**
   * Creates a [[DataBag]] codec from an element column.
   * Note: data must be collected before encoding.
   */
  implicit def dataBagCSVConverter[A: CSVColumn: Meta]: CSVConverter[DataBag[A]] =
    iso[Seq[A], DataBag[A]]
}
