package eu.stratosphere
package emma.macros

import emma.compiler

import scala.reflect.macros.blackbox

/** Automatic derivation of CSV encoding / decoding for primitives and [[scala.Product]] types. */
// TODO: add DateTime support
class ConvertersMacros(val c: blackbox.Context) extends compiler.MacroUtil {

  import universe._
  import Emma._
  import Term._
  import Tree._
  import Type._
  import Term.name.fresh

  private val iter = fresh("iter")
  private val const = q"$Scala.Function.const"
  private val seq = q"$Scala.Seq"

  def materializeCSVConverters[T: c.WeakTypeTag] = Type.check {
    val T = Type.weak[T]
    val value = fresh("value")
    val sep = fresh("sep")

    val from = q"""($value: ${array(string)}) => {
      val $iter = $value.iterator
      ${fromCSV(T, q"$iter.next")}
    }"""

    val to = q"""($value: $T, $sep: $char) =>
      ${toCSV(T, q"$value")}.toArray
    """

    q"""$emma.api.CSVConverters[$T]($from)($to)"""
  }

  def fromCSV(T: Type, value: Tree): Tree =
    if (T <:< Type[Product]) {
      val method = T.decl(Term.name.init).alternatives.head.asMethod
      val params = method.typeSignatureIn(T).paramLists.head
      val args = for (p <- params) yield fromCSV(Type of p, value)
      q"new $T(..$args)"
    } else {
      if (T =:= Type.unit || T =:= void) Term.unit
      else if (T =:= bool || T =:= jBool) q"$value.toBoolean"
      else if (T =:= char || T =:= jChar) q"$value.head"
      else if (T =:= byte || T =:= jByte) q"$value.toByte"
      else if (T =:= short || T =:= jShort) q"$value.toShort"
      else if (T =:= int || T =:= jInt) q"$value.toInt"
      else if (T =:= long || T =:= jLong) q"$value.toLong"
      else if (T =:= float || T =:= jFloat) q"$value.toFloat"
      else if (T =:= double || T =:= jDouble) q"$value.toDouble"
      else if (T =:= bigInt) q"$Scala.BigInt($value)"
      else if (T =:= bigDec) q"$Scala.BigDecimal($value)"
      else if (T =:= jBigInt) q"new $Java.math.BigInteger($value)"
      else if (T =:= jBigDec) q"new $Java.math.BigDecimal($value)"
      else if (T =:= string) q"$value"
      else q"$const(${null_(T)})($value)"
    }

  def toCSV(T: Type, value: Tree): Tree = {
    if (T <:< Type[Product]) {
      val method = T.decl(Term.name.init).alternatives.head.asMethod
      val params = method.typeSignatureIn(T).paramLists.head
      val fields = for {
        param <- params
        method <- T.members
        if method.isMethod
        if method.asMethod.isGetter
        if method.toString == param.toString
      } yield toCSV(result(method.infoIn(T)), q"$value.$method")

      q"$seq(..$fields).flatten"
    } else {
      q"$seq($value.toString)"
    }
  }
}
