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
import shapeless.ops.hlist._
import shapeless.ops.record._

/** Implicit [[ParquetConverter]] privder. */
trait MkParquetConverter[A] extends (() => ParquetConverter[A])

/** Derived [[ParquetConverter]] instances. */
object MkParquetConverter {
  import util.Default
  import ParquetConverter._

  /** Lazily make a [[ParquetConverter]] instance. */
  private def mk[A](instance: => ParquetConverter[A]): MkParquetConverter[A] =
    new MkParquetConverter[A] { def apply = instance }

  /**
   * Generic Parquet codec for case classes and tuples (aka product types).
   * @param gen A Converter between [[A]] and its generic representation [[R]].
   * @param nonEmpty Evidence that [[A]] has at least one field.
   * @param unzip A dependent function that unzips the field names [[K]] and field values [[V]] from [[R]].
   * @param record A dependent Function that zips a list of values [[V]] with the field names [[K]] back into [[R]].
   * @param converters Resolved implicit [[ParquetConverter]] instances for the value types in [[V]].
   * @param defaults Resolved implicit [[Default]] instances for the value types in [[V]].
   * @param keysArr `toArray[Symbol]` for the heterogeneous field names list [[K]].
   * @param valsArr `toArray[Any]` for the heterogeneous values list [[V]].
   * @param convArr `toArray[ParquetConverter[_]]` for the `converters` instances.
   * @param defsArr `toArray[Default[_]]` for the `defaults` instances.
   * @tparam A The product type for which a Parquet codec is being derived.
   * @tparam R The generic record representation of [[A]], labelled with field names.
   * @tparam K For each field in [[A]], the name of that field as a literal [[Symbol]] type.
   * @tparam V For each field in [[A]], the type of its value, in the same order as [[K]].
   * @tparam P For each value in [[V]], the resolved implicit [[ParquetConverter]] instance.
   * @tparam D For each value in [[V]], the resolved implicit [[Default]] instance.
   * @return A derived [[ParquetConverter]] instance for [[A]].
   */
  implicit def product[
    A, R <: HList, K <: HList, V <: HList, P <: HList, D <: HList
  ](implicit
    gen: LabelledGeneric.Aux[A, R],
    nonEmpty: IsHCons[R],
    unzip: UnzipFields.Aux[R, K, V],
    record: ZipWithKeys.Aux[K, V, R],
    converters: LiftAll.Aux[ParquetConverter, V, P],
    defaults: LiftAll.Aux[Default, V, D],
    keysArr: ToArray[K, Symbol],
    valsArr: ToArray[V, Any],
    convArr: ToArray[P, ParquetConverter[_]],
    defsArr: ToArray[D, Default[_]]
  ): MkParquetConverter[A] = mk(new ParquetConverter[A] {
    val keys = for (k <- keysArr(unzip.keys)) yield k.name
    val conv = convArr(converters.instances).asInstanceOf[Array[ParquetConverter[Any]]]
    val defs = defsArr(defaults.instances).asInstanceOf[Array[Default[Any]]]
    val indx = keys.indices

    val schema = new GroupType(OPTIONAL, defaultName,
      (for (i <- indx) yield copy(conv(i).schema)(name = keys(i))): _*)

    def reader(read: A => Unit, top: Boolean) = new GroupConverter {
      val values  = for (d <- defs) yield d.value
      val readers = for (i <- indx) yield conv(i).reader(values(i) = _)
      def getConverter(i: Int) = readers(i)
      def start() = for (i <- indx) values(i) = defs(i).value
      def end() = read(gen.from(record(values.foldRight[HList](HNil)(_ :: _).asInstanceOf[V])))
    }

    def writer(consumer: RecordConsumer, top: Boolean) = {
      val writers = for (c <- conv) yield c.writer(consumer)
      def all(values: Array[Any]) = for (i <- indx)
        if (!defs(i).empty(values(i))) field(consumer, keys(i), i) {
          writers(i)(values(i))
        }

      value => if (value != null) {
        val values = valsArr(unzip.values(gen.to(value)))
        if (top) message(consumer) { all(values) }
        else group(consumer) { all(values) }
      }
    }
  })
}
