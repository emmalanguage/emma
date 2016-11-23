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

import ast.MacroAST

import scala.reflect.macros.blackbox

/** Automatic derivation of CSV encoding / decoding for primitives and [[scala.Product]] types. */
// TODO: add DateTime support
class CSVConverterMacro(val c: blackbox.Context) extends MacroAST {

  import universe._
  import api.Type._

  private val iter = api.TermName.fresh("iter")
  private val const = q"${api.Tree.Scala}.Function.const"
  private val seq = q"${api.Tree.Scala}.collection.Seq"
  private val emma = q"_root_.org.emmalanguage"

  def materialize[T: c.WeakTypeTag] = typeCheck {
    val T = api.Type.weak[T]
    val value = api.TermName.fresh("value")

    val from =
      q"""
        ($value: ${arrayOf(string)}) => {
          val $iter = $value.iterator
          ${fromCSV(T, q"$iter.next")}
        }
       """

    val to =
      q"""
        ($value: $T) => {
          ${toCSV(T, q"$value")}.toArray
        }
       """

    q"""$emma.io.csv.CSVConverter[$T]($from)($to)"""
  }

  def fromCSV(T: Type, value: Tree): Tree =
    if (T <:< api.Type[Product]) {
      val alternatives = T.decl(api.TermName.init).alternatives
      if (alternatives.isEmpty) {
        throw new Exception("fromCSV failed. Did you forget to add explicit generic argument to readCSV?")
      }
      val method = alternatives.head.asMethod
      val params = method.infoIn(T).paramLists.head
      val args = for (p <- params) yield fromCSV(p.info, value)
      q"new $T(..$args)"
    } else {
      if (T =:= unit || T =:= Java.void) api.Term.unit
      else if (T =:= bool || T =:= Java.bool) q"$value.toBoolean"
      else if (T =:= char || T =:= Java.char) q"$value.head"
      else if (T =:= byte || T =:= Java.byte) q"$value.toByte"
      else if (T =:= short || T =:= Java.short) q"$value.toShort"
      else if (T =:= int || T =:= Java.int) q"$value.toInt"
      else if (T =:= long || T =:= Java.long) q"$value.toLong"
      else if (T =:= float || T =:= Java.float) q"$value.toFloat"
      else if (T =:= double || T =:= Java.double) q"$value.toDouble"
      else if (T =:= bigInt) q"${api.Tree.Scala}.BigInt($value)"
      else if (T =:= bigDec) q"${api.Tree.Scala}.BigDecimal($value)"
      else if (T =:= Java.bigInt) q"new ${api.Tree.Java}.math.BigInteger($value)"
      else if (T =:= Java.bigDec) q"new ${api.Tree.Java}.math.BigDecimal($value)"
      else if (T =:= string) q"$value"
      else q"$const(null.asInstanceOf[$T])($value)"
    }

  def toCSV(T: Type, value: Tree): Tree = {
    if (T <:< api.Type[Product]) {
      val method = T.decl(api.TermName.init).alternatives.head.asMethod
      val params = method.infoIn(T).paramLists.head
      val fields = for {
        param <- params
        method <- T.members
        if method.isMethod
        if method.asMethod.isGetter
        if method.toString == param.toString
      } yield toCSV(method.infoIn(T).finalResultType, q"$value.$method")

      q"$seq(..$fields).flatten"
    } else {
      q"$seq($value.toString)"
    }
  }
}
