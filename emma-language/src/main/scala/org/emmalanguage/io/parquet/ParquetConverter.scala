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

import util._

import org.apache.parquet.io.api._
import org.apache.parquet.schema._
import Type.Repetition._

import shapeless._
import shapeless.labelled._
import shapeless.ops.hlist._
import shapeless.ops.nat._
import shapeless.tag._

import scala.collection.generic.CanBuild
import scala.language.higherKinds

/**
 * Codec (provides encoding and decoding) type-class for the Parquet columnar format.
 *
 * Provided by default are codecs for Scala and Java primitives, collections and enumerations,
 * [[Option]], tuples and case classes.
 */
trait ParquetConverter[A] {
  def schema: Type
  def reader(read: A => Unit): Converter
  def writer(consumer: RecordConsumer): A => Unit
}

/** Factory methods and implicit instances of Parquet codecs. */
object ParquetConverter extends IsoParquetConverters {

  /** Summons an implicit codec for type `A` available in scope. */
  def apply[A](implicit converter: ParquetConverter[A]): ParquetConverter[A] =
    converter
}

trait IsoParquetConverters extends GenericParquetConverters {

  private implicit val binaryIsoArrayByte: Binary <=> Array[Byte] =
    Iso.make(_.getBytes, Binary.fromByteArray)

  private implicit val binaryIsoByte: Binary <=> Byte =
    Iso.make(_.getBytes.head, b => Binary.fromByteArray(Array(b)))

  private implicit val binaryIsoShort: Binary <=> Short =
    Iso.make(bin => {
      val bytes = bin.getBytes
      (bytes(0) + (bytes(1) << 8)).toShort
    }, short => {
      val bytes = Array(short.toByte, (short >> 8).toByte)
      Binary.fromByteArray(bytes)
    })

  private implicit val binaryIsoString: Binary <=> String =
    Iso.make(_.toStringUsingUTF8, Binary.fromString)

  private implicit val shortIsoChar: Short <=> Char =
    Iso.make(_.toChar, _.toShort)

  /**
   * Derives a new Parquet codec for type `B` from an existing codec for type `A`,
   * given an (implicit) isomorphism between `A` and `B`.
   *
   * Handles Java primitives and collections
   * (the latter requires `import scala.collection.JavaConversions._`).
   */
  implicit def iso[A, B](implicit iso: A <=> B, underlying: ParquetConverter[A])
  : ParquetConverter[B] = new ParquetConverter[B] {
    def schema = underlying.schema
    def reader(read: B => Unit) = underlying.reader(read.compose(iso.from))
    def writer(consumer: RecordConsumer) = underlying.writer(consumer).compose(iso.to)
  }

  // Isomorphic converters
  implicit val byteParquetConverter:      ParquetConverter[Byte]        = iso[Binary, Byte]
  implicit val shortParquetConverter:     ParquetConverter[Short]       = iso[Binary, Short]
  implicit val charParquetConverter:      ParquetConverter[Char]        = iso[Short, Char]
  implicit val stringParquetConverter:    ParquetConverter[String]      = iso[Binary, String]
  implicit val byteArrayParquetConverter: ParquetConverter[Array[Byte]] = iso[Binary, Array[Byte]]
}

trait GenericParquetConverters extends PrimitiveParquetConverters {

  /** Creates an optional Parquet codec from an existing codec. */
  implicit def optionParquetConverter[A](
    implicit A: ParquetConverter[A]
  ): ParquetConverter[Option[A]] = new ParquetConverter[Option[A]] {
    val schema = new GroupType(REQUIRED, defaultField, copy(A.schema)(arity = OPTIONAL))

    def reader(read: Option[A] => Unit) = new GroupConverter {
      var optional = Option.empty[A]
      val underlying = A.reader(v => optional = Some(v))
      def getConverter(i: Int) = underlying
      def start() = optional = None
      def end() = read(optional)
    }

    def writer(consumer: RecordConsumer) = {
      val write = A.writer(consumer)
      optional => {
        consumer.startGroup()
        for (value <- optional) {
          consumer.startField(defaultField, 0)
          write(value)
          consumer.endField(defaultField, 0)
        }
        consumer.endGroup()
      }
    }
  }

  /** Creates Parquet codec for a [[Traversable]] collection from an element codec. */
  implicit def traversableParquetConverter[T[x] <: Traversable[x], A](
    implicit TA: CanBuild[A, T[A]], A: ParquetConverter[A]
  ): ParquetConverter[T[A]] = new ParquetConverter[T[A]] {
    val schema = new GroupType(REQUIRED, defaultField, copy(A.schema)(arity = REPEATED))

    def reader(read: T[A] => Unit) = new GroupConverter {
      val builder = TA()
      val element = A.reader(builder += _)
      def getConverter(i: Int) = element
      def start() = builder.clear()
      def end() = read(builder.result())
    }

    def writer(consumer: RecordConsumer) = {
      val write = A.writer(consumer)
      values => {
        consumer.startGroup()
        consumer.startField(defaultField, 0)
        values.foreach(write)
        consumer.endField(defaultField, 0)
        consumer.endGroup()
      }
    }
  }

  /**
   * Creates a labelled Parquet codec for a generic field.
   * @tparam K The singleton label symbol used as a field name in the schema.
   * @tparam V The underlying type of the field with an existing codec.
   * @tparam N The natural number type specifying the index of the field in a group.
   */
  implicit def fieldParquetConverter[K <: Symbol, V, N <: Nat](
    implicit K: Witness.Aux[K], V: ParquetConverter[V], toInt: ToInt[N]
  ): ParquetConverter[FieldType[K, V] @@ N] = new ParquetConverter[FieldType[K, V] @@ N] {
    def key = K.value.name
    val idx = toInt()
    val schema = copy(V.schema)(name = key)

    def reader(read: FieldType[K, V] @@ N => Unit) =
      V.reader(read.compose(v => tag[N](field[K](v))))

    def writer(consumer: RecordConsumer) = {
      val write = V.writer(consumer)
      value => {
        consumer.startField(key, idx)
        write(value)
        consumer.endField(key, idx)
      }
    }
  }

  /** Induction base for deriving generic Parquet codecs (an empty group is not allowed). */
  implicit def hConsParquetConverter[H](
    implicit H: Lazy[ParquetConverter[H]]
  ): ParquetConverter[H :: HNil] = new ParquetConverter[H :: HNil] {
    def head = H.value
    val schema = new GroupType(REQUIRED, defaultField, head.schema)

    def reader(read: H :: HNil => Unit) = new GroupConverter {
      val underlying = head.reader(read.compose(_ :: HNil))
      def getConverter(i: Int) = underlying
      def start() = ()
      def end() = ()
    }

    def writer(consumer: RecordConsumer) =
      head.writer(consumer).compose(_.head)
  }

  /** Induction step for deriving generic Parquet codecs. */
  implicit def hListParquetConverter[A, B, T <: HList](
    implicit H: Lazy[ParquetConverter[A]], T: Lazy[ParquetConverter[B :: T]]
  ): ParquetConverter[A :: B :: T] = new ParquetConverter[A :: B :: T] {
    def head = H.value
    def tail = T.value

    val schema = {
      val underlying = tail.schema.asGroupType
      val fields = new java.util.ArrayList[Type](underlying.getFieldCount + 1)
      fields.add(head.schema)
      fields.addAll(underlying.getFields)
      new GroupType(REQUIRED, defaultField, fields)
    }

    def reader(read: A :: B :: T => Unit) = new GroupConverter {
      var value: A = _
      val hReader = head.reader(value = _)
      val tReader = tail.reader(read.compose(value :: _)).asGroupConverter
      def getConverter(i: Int) = if (i == 0) hReader else tReader.getConverter(i - 1)
      def start() = ()
      def end() = ()
    }

    def writer(consumer: RecordConsumer) = {
      val writeH = head.writer(consumer)
      val writeT = tail.writer(consumer)
      value => {
        writeH(value.head)
        writeT(value.tail)
      }
    }
  }

  /** Polymorphic function for tagging generic fields. */
  object tagWith extends Poly1 {
    implicit def tagField[K, V, T] =
      at[(FieldType[K, V], T)] { case (kv, _) => tag[T](kv) }
  }

  /** Polymorphic function for stripping tagged fields. */
  object stripTag extends Poly1 {
    implicit def stripField[K, V, T] =
      at[FieldType[K, V] @@ T](identity[FieldType[K, V]])
  }

  /** Generic Parquet codec for case classes and tuples. */
  implicit def genericParquetConverter[A, R <: HList, I <: HList, T <: HList](implicit
    generic: LabelledGeneric.Aux[A, R],
    indexed: ZipWithIndex.Aux[R, I],
    tagged:  Mapper.Aux[tagWith.type, I, T],
    strip:   Mapper.Aux[stripTag.type, T, R],
    T: Lazy[ParquetConverter[T]]
  ): ParquetConverter[A] = new ParquetConverter[A] {
    def underlying = T.value
    def schema = underlying.schema

    def reader(read: A => Unit) = underlying
      .reader(v => read(generic.from(strip(v))))

    def writer(consumer: RecordConsumer) = {
      val write = underlying.writer(consumer)
      value => {
        consumer.startGroup()
        write(tagged(indexed(generic.to(value))))
        consumer.endGroup()
      }
    }
  }
}

trait PrimitiveParquetConverters {
  import PrimitiveType.PrimitiveTypeName._

  /** The default field name used for group wrappers. */
  val defaultField = "value"

  /** Copies a Parquet `schema`, changing its repetition or name. */
  protected def copy(schema: Type)(
    arity: Type.Repetition = schema.getRepetition,
    name:  String          = schema.getName
  ): Type = if (schema.isPrimitive) {
    val prim = schema.asPrimitiveType
    new PrimitiveType(arity, prim.getPrimitiveTypeName, name)
  } else {
    val group = schema.asGroupType
    new GroupType(arity, name, group.getFields)
  }

  /** Codec for raw binary Parquet data. */
  implicit val binaryParquetConverter: ParquetConverter[Binary] = new ParquetConverter[Binary] {
    val schema = new PrimitiveType(REQUIRED, BINARY, defaultField)
    def writer(consumer: RecordConsumer) = consumer.addBinary
    def reader(read: Binary => Unit) = new PrimitiveConverter {
      override def addBinary(value: Binary) = read(value)
    }
  }

  implicit val booleanParquetConverter: ParquetConverter[Boolean] = new ParquetConverter[Boolean] {
    val schema = new PrimitiveType(REQUIRED, BOOLEAN, defaultField)
    def writer(consumer: RecordConsumer) = consumer.addBoolean
    def reader(read: Boolean => Unit) = new PrimitiveConverter {
      override def addBoolean(value: Boolean) = read(value)
    }
  }

  implicit val intParquetConverter: ParquetConverter[Int] = new ParquetConverter[Int] {
    val schema = new PrimitiveType(REQUIRED, INT32, defaultField)
    def writer(consumer: RecordConsumer) = consumer.addInteger
    def reader(read: Int => Unit) = new PrimitiveConverter {
      override def addInt(value: Int) = read(value)
    }
  }

  implicit val longParquetConverter: ParquetConverter[Long] = new ParquetConverter[Long] {
    val schema = new PrimitiveType(REQUIRED, INT64, defaultField)
    def writer(consumer: RecordConsumer) = consumer.addLong
    def reader(read: Long => Unit) = new PrimitiveConverter {
      override def addLong(value: Long) = read(value)
    }
  }

  implicit val floatParquetConverter: ParquetConverter[Float] = new ParquetConverter[Float] {
    val schema = new PrimitiveType(REQUIRED, FLOAT, defaultField)
    def writer(consumer: RecordConsumer) = consumer.addFloat
    def reader(read: Float => Unit) = new PrimitiveConverter {
      override def addFloat(value: Float) = read(value)
    }
  }

  implicit val doubleParquetConverter: ParquetConverter[Double] = new ParquetConverter[Double] {
    val schema = new PrimitiveType(REQUIRED, DOUBLE, defaultField)
    def writer(consumer: RecordConsumer) = consumer.addDouble
    def reader(read: Double => Unit) = new PrimitiveConverter {
      override def addDouble(value: Double) = read(value)
    }
  }
}
