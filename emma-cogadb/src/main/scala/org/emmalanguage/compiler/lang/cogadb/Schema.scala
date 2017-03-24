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
package compiler.lang.cogadb

import compiler.lang.cogadb.ast.SchemaAttr

import net.liftweb.json.JsonAST.JObject

trait CoGaDBType[T] {
  def tpe: String
}

object CoGaDBType {
  implicit val IntCoGaDBType = new CoGaDBType[Int] {
    val tpe = "INT"
  }
  implicit val DoubleCoGaDBType = new CoGaDBType[Double] {
    val tpe = "DOUBLE"
  }
  implicit val FloatCogaDBType = new CoGaDBType[Float] {
    val tpe = "FLOAT"
  }
  implicit val CharCogaDBType = new CoGaDBType[Char] {
    val tpe = "CHAR"
  }
  implicit val StringCogaDBType = new CoGaDBType[String] {
    val tpe = "VARCHAR"
  }
}

trait Schema[T] {

  def fields(): Seq[SchemaAttr]


}

object Schema {
  implicit def SimpleSchema[T: CoGaDBType] = new Schema[T] {
    def fields(): Seq[SchemaAttr] =
      Seq(SchemaAttr(implicitly[CoGaDBType[T]].tpe, "value"))
  }

  //TODO: All types

  implicit def Tuple2Schema[T1, T2](implicit
    T1Schema: CoGaDBType[T1],
    T2Schema: CoGaDBType[T2]
  ) = new Schema[(T1, T2)] {
    def fields(): Seq[SchemaAttr] = Seq(
      SchemaAttr(T1Schema.tpe, "_1"),
      SchemaAttr(T2Schema.tpe, "_2"))
  }

  implicit def Tuple3Schema[T1, T2, T3](implicit
    T1Schema: CoGaDBType[T1],
    T2Schema: CoGaDBType[T2],
    T3Schema: CoGaDBType[T3]
  ) = new Schema[(T1, T2, T3)] {
    def fields(): Seq[SchemaAttr] = Seq(
      SchemaAttr(T1Schema.tpe, "_1"),
      SchemaAttr(T2Schema.tpe, "_2"),
      SchemaAttr(T3Schema.tpe, "_3"))
  }

  implicit def Tuple4Schema[T1, T2, T3, T4](implicit
    T1Schema: CoGaDBType[T1],
    T2Schema: CoGaDBType[T2],
    T3Schema: CoGaDBType[T3],
    T4Schema: CoGaDBType[T4]
  ) = new Schema[(T1, T2, T3, T4)] {
    def fields(): Seq[SchemaAttr] = Seq(
      SchemaAttr(T1Schema.tpe, "_1"),
      SchemaAttr(T2Schema.tpe, "_2"),
      SchemaAttr(T3Schema.tpe, "_3"),
      SchemaAttr(T4Schema.tpe, "_4"))
  }

  implicit def Tuple5Schema[T1, T2, T3, T4, T5](implicit
    T1Schema: CoGaDBType[T1],
    T2Schema: CoGaDBType[T2],
    T3Schema: CoGaDBType[T3],
    T4Schema: CoGaDBType[T4],
    T5Schema: CoGaDBType[T5]
  ) = new Schema[(T1, T2, T3, T4, T5)] {
    def fields(): Seq[SchemaAttr] = Seq(
      SchemaAttr(T1Schema.tpe, "_1"),
      SchemaAttr(T2Schema.tpe, "_2"),
      SchemaAttr(T3Schema.tpe, "_3"),
      SchemaAttr(T4Schema.tpe, "_4"),
      SchemaAttr(T5Schema.tpe, "_5"))
  }

  implicit def Tuple6Schema[T1, T2, T3, T4, T5, T6](implicit
    T1Schema: CoGaDBType[T1],
    T2Schema: CoGaDBType[T2],
    T3Schema: CoGaDBType[T3],
    T4Schema: CoGaDBType[T4],
    T5Schema: CoGaDBType[T5],
    T6Schema: CoGaDBType[T6]
  ) = new Schema[(T1, T2, T3, T4, T5, T6)] {
    def fields(): Seq[SchemaAttr] = Seq(
      SchemaAttr(T1Schema.tpe, "_1"),
      SchemaAttr(T2Schema.tpe, "_2"),
      SchemaAttr(T3Schema.tpe, "_3"),
      SchemaAttr(T4Schema.tpe, "_4"),
      SchemaAttr(T5Schema.tpe, "_5"),
      SchemaAttr(T6Schema.tpe, "_6"))
  }
}