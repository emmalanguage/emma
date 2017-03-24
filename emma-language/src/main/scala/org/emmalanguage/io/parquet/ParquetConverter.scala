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
package io.parquet

import org.apache.parquet.io.api._
import org.apache.parquet.schema._
import Type.Repetition._

import shapeless._

import scala.collection.JavaConversions._
import scala.collection.generic.CanBuild
import scala.language.higherKinds
import scala.reflect.ClassTag

import java.util.Date

/**
 * Codec (provides encoding and decoding) type-class for the Parquet columnar format.
 *
 * Provided by default are codecs for Scala and Java primitives, collections and enumerations,
 * [[Option]], tuples and case classes.
 */
trait ParquetConverter[A] {
  def schema: Type
  def reader(read: A => Unit, top: Boolean = false): Converter
  def writer(consumer: RecordConsumer, top: Boolean = false): A => Unit
}

/** Factory methods and implicit instances of Parquet codecs. */
object ParquetConverter extends IsomorphicParquetConverters {

  /** Summons an implicit codec for type `A` available in scope. */
  def apply[A: ParquetConverter]: ParquetConverter[A] = implicitly
}

/** Isomorphic [[ParquetConverter]] instances. */
trait IsomorphicParquetConverters extends PrimitiveParquetConverters {
  import util.Default
  import OriginalType._

  /**
   * Derives a new Parquet codec for type `B` from an existing codec for type `A`,
   * given an (implicit) isomorphism between `A` and `B`.
   */
  def isomorphic[A, B](from: A => B, to: B => A, original: OriginalType = null)(
    implicit underlying: ParquetConverter[A], default: Default[B]
  ): ParquetConverter[B] = new ParquetConverter[B] {
    val schema = {
      val schema = underlying.schema
      val arity  = schema.getRepetition
      copy(schema)(
        arity     = if (arity == REQUIRED && default.nullable) OPTIONAL else arity,
        original  = if (original == null) schema.getOriginalType else original)
    }

    def reader(read: B => Unit, top: Boolean) =
      underlying.reader(read.compose(from), top)

    def writer(consumer: RecordConsumer, top: Boolean) = {
      val write = underlying.writer(consumer, top)
      value => if (!default.empty(value)) write(to(value))
    }
  }

  // Isomorphic parquet converters

  implicit val byte: ParquetConverter[Byte] =
    isomorphic[Int, Byte](_.toByte, _.toInt, original = INT_8)

  implicit val short: ParquetConverter[Short] =
    isomorphic[Int, Short](_.toShort, _.toInt, original = INT_16)

  implicit val char: ParquetConverter[Char] =
    isomorphic[Int, Char](_.toChar, _.toInt, original = INT_16)

  implicit val string: ParquetConverter[String] =
    isomorphic[Binary, String](_.toStringUsingUTF8, Binary.fromString, original = UTF8)

  implicit val symbol: ParquetConverter[Symbol] =
    isomorphic[String, Symbol](Symbol.apply, _.name)

  implicit val bigInt: ParquetConverter[BigInt] =
    isomorphic[String, BigInt](BigInt.apply, _.toString)

  implicit val bigDec: ParquetConverter[BigDecimal] =
    isomorphic[String, BigDecimal](BigDecimal.apply, _.toString)

  implicit val date: ParquetConverter[Date] =
    isomorphic[Long, Date](new Date(_), _.getTime, original = DATE)

  implicit def array[E: ParquetConverter: Default: ClassTag]: ParquetConverter[Array[E]] =
    isomorphic[Seq[E], Array[E]](_.toArray, _.toSeq, original = LIST)

  implicit def map[K, V](
    implicit kv: ParquetConverter[(K, V)], default: Default[(K, V)]
  ): ParquetConverter[Map[K, V]] = isomorphic[Seq[(K, V)], Map[K, V]](_.toMap, _.toSeq, original = MAP)
}

trait PrimitiveParquetConverters extends GenericParquetConverters {
  import PrimitiveType.PrimitiveTypeName
  import PrimitiveTypeName._

  private def primitive[A](tpe: PrimitiveTypeName)(
    mkReader: (A => Unit) => PrimitiveConverter,
    mkWriter: RecordConsumer => (A => Unit)
  ): ParquetConverter[A] = new ParquetConverter[A] {
    val schema = new PrimitiveType(REQUIRED, tpe, defaultName)

    def reader(read: A => Unit, top: Boolean) = {
      val reader = mkReader(read)
      if (top) new GroupConverter {
        def getConverter(i: Int) = reader
        def start() = ()
        def end() = ()
      } else reader
    }

    def writer(consumer: RecordConsumer, top: Boolean) = {
      val write = mkWriter(consumer)
      if (top) value => message(consumer) {
        field(consumer) { write(value) }
      } else write
    }
  }

  /** Codec for raw binary Parquet data. */
  implicit val binary: ParquetConverter[Binary] =
    primitive(BINARY)(read => new PrimitiveConverter {
      override def addBinary(value: Binary) = read(value)
    }, _.addBinary)

  implicit val boolean: ParquetConverter[Boolean] =
    primitive(BOOLEAN)(read => new PrimitiveConverter {
      override def addBoolean(value: Boolean) = read(value)
    }, _.addBoolean)

  implicit val int: ParquetConverter[Int] =
    primitive(INT32)(read => new PrimitiveConverter {
      override def addInt(value: Int) = read(value)
    }, _.addInteger)

  implicit val long: ParquetConverter[Long] =
    primitive(INT64)(read => new PrimitiveConverter {
      override def addLong(value: Long) = read(value)
    }, _.addLong)

  implicit val float: ParquetConverter[Float] =
    primitive(FLOAT)(read => new PrimitiveConverter {
      override def addFloat(value: Float) = read(value)
    }, _.addFloat)

  implicit val double: ParquetConverter[Double] =
    primitive(DOUBLE)(read => new PrimitiveConverter {
      override def addDouble(value: Double) = read(value)
    }, _.addDouble)
}

/** Generic [[ParquetConverter]] instances. */
trait GenericParquetConverters extends DefaultParquetConverters {
  import util.Default

  /** Creates an optional Parquet codec from an existing codec. */
  implicit def optional[A](
    implicit underlying: ParquetConverter[A]
  ): ParquetConverter[Option[A]] = new ParquetConverter[Option[A]] {
    val schema = copy(underlying.schema)(arity = OPTIONAL)
    def reader(read: Option[A] => Unit, top: Boolean) =
      underlying.reader(read.compose(Option.apply), top)
    def writer(consumer: RecordConsumer, top: Boolean) =
      _.foreach(underlying.writer(consumer, top))
  }

  /** Creates Parquet codec for a [[Traversable]] collection from an element codec. */
  implicit def traversable[T[x] <: Traversable[x], E](
    implicit element: ParquetConverter[E], default: Default[E], cbf: CanBuild[E, T[E]]
  ): ParquetConverter[T[E]] = new ParquetConverter[T[E]] {
    val schema = GroupSchema(OPTIONAL, defaultName, OriginalType.LIST,
      new GroupType(REPEATED, repeatedName, copy(element.schema)(name = elementName)))

    def reader(read: T[E] => Unit, top: Boolean) = new GroupConverter {
      val builder = cbf()
      val underlying: Converter = new GroupConverter {
        var current = default.value
        val underlying = element.reader(current = _)
        def getConverter(i: Int) = underlying
        def start() = current = default.value
        def end() = builder += current
      }

      def getConverter(i: Int) = underlying
      def start() = builder.clear()
      def end() = read(builder.result())
    }

    def writer(consumer: RecordConsumer, top: Boolean) = {
      val write = element.writer(consumer)
      def all(values: T[E]) = field(consumer, repeatedName) {
        for (value <- values) group(consumer) {
          if (!default.empty(value)) {
            field(consumer, elementName) { write(value) }
          }
        }
      }

      values => if (values != null && values.nonEmpty) {
        if (top) message(consumer) { all(values) }
        else group(consumer) { all(values) }
      }
    }
  }
}

/** Lowest priority [[ParquetConverter]] instances. */
trait DefaultParquetConverters {
  val defaultName  = "value"
  val elementName  = "element"
  val repeatedName = "list"

  // Workaround to suppress deprecated warning.
  protected object GroupSchema extends GroupSchema
  @deprecated("", "") protected trait GroupSchema {
    def apply(arity: Type.Repetition, name: String, original: OriginalType, fields: Type*): GroupType =
      new GroupType(arity, name, original, fields: _*)
  }

  /** Copies a Parquet `schema`, changing its repetition or name. */
  def copy(schema: Type)(
    arity:    Type.Repetition = schema.getRepetition,
    name:     String          = schema.getName,
    original: OriginalType    = schema.getOriginalType
  ): Type = if (schema.isPrimitive) {
    new PrimitiveType(arity, schema.asPrimitiveType.getPrimitiveTypeName, name, original)
  } else {
    GroupSchema(arity, name, original, schema.asGroupType.getFields: _*)
  }

  /** Helper method for writing a message. */
  def message(consumer: RecordConsumer)(write: => Unit): Unit = {
    consumer.startMessage()
    write
    consumer.endMessage()
  }

  /** Helper method for writing a group. */
  def group(consumer: RecordConsumer)(write: => Unit): Unit = {
    consumer.startGroup()
    write
    consumer.endGroup()
  }

  /** Helper method for writing a field. */
  def field(consumer: RecordConsumer,
    name: String = defaultName, index: Int = 0
  )(write: => Unit): Unit = {
    consumer.startField(name, index)
    write
    consumer.endField(name, index)
  }

  /** Resolves only when there is no other implicit [[ParquetConverter]] in scope. */
  implicit def derived[A](
    implicit lp: LowPriority, mk: Strict[MkParquetConverter[A]]
  ): ParquetConverter[A] = mk.value()
}
