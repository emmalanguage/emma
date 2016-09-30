package org.emmalanguage
package macros

import ast.MacroAST

import scala.reflect.macros.blackbox

/** Automatic derivation of CSV encoding / decoding for primitives and [[scala.Product]] types. */
// TODO: add DateTime support
class ConvertersMacros(val c: blackbox.Context) extends MacroAST {

  import universe._
  import api.Type._

  private val iter = api.TermName.fresh("iter")
  private val const = q"${api.Tree.Scala}.Function.const"
  private val seq = q"${api.Tree.Scala}.collection.Seq"
  private val emma = q"_root_.eu.stratosphere.emma"

  def materializeCSVConverters[T: c.WeakTypeTag] = api.Type.check {
    val T = api.Type.weak[T]
    val value = api.TermName.fresh("value")
    val sep = api.TermName.fresh("sep")

    val from = q"""($value: ${arrayOf(string)}) => {
      val $iter = $value.iterator
      ${fromCSV(T, q"$iter.next")}
    }"""

    val to = q"""($value: $T, $sep: $char) =>
      ${toCSV(T, q"$value")}.toArray
    """

    q"""$emma.api.CSVConverters[$T]($from)($to)"""
  }

  def fromCSV(T: Type, value: Tree): Tree =
    if (T <:< api.Type[Product]) {
      val method = T.decl(api.TermName.init).alternatives.head.asMethod
      val params = method.typeSignatureIn(T).paramLists.head
      val args = for (p <- params) yield fromCSV(api.Type.of(p), value)
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
      val params = method.typeSignatureIn(T).paramLists.head
      val fields = for {
        param <- params
        method <- T.members
        if method.isMethod
        if method.asMethod.isGetter
        if method.toString == param.toString
      } yield toCSV(api.Type.result(method.infoIn(T)), q"$value.$method")

      q"$seq(..$fields).flatten"
    } else {
      q"$seq($value.toString)"
    }
  }
}
