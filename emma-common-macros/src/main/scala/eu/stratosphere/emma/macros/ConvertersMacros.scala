package eu.stratosphere
package emma.macros

import emma.compiler

import scala.reflect.macros.blackbox

/** Automatic derivation of CSV encoding / decoding for primitives and [[Product]] types. */
// TODO: add DateTime support
class ConvertersMacros(val c: blackbox.Context) extends compiler.MacroUtil {

  import universe._
  import Emma._
  import Tree._
  import Type._
  import Term.name.fresh

  private val builder =
    fresh("builder")

  def materializeCSVConverters[T: c.WeakTypeTag] = Type.check {
    val T = Type.weak[T]
    val value = fresh("value")
    val index = fresh("index")
    val sep = fresh("sep")

    val from = q"""
      ($value: ${array(string)}) => {
        var $index = -1
        ${fromCSV(T, q"$value", q"{ $index += 1; $index }")}
      }"""

    val to = q"""
      ($value: $T, $sep: $char) => {
        $builder.clear()
        ${toCSV(T, q"$value")}
        $builder.result()
      }"""

    q"""{
      val $builder = $Scala.collection.mutable.ArrayBuilder.make[$string]
      $emma.api.CSVConverters[$T]($from)($to)
    }"""
  }

  def fromCSV(T: Type, value: Tree, index: Tree): Tree =
    if (T <:< Type[Product]) {
      val method = T.decl(Term.name.init).alternatives.head.asMethod
      val params = method.typeSignatureIn(T).paramLists.head
      val args = for (p <- params) yield fromCSV(Type.of(p), value, index)
      q"new $T(..$args)"
    } else {
      if (T =:= Type.unit || T =:= void) Tree.unit
      else if (T =:= bool || T =:= jBool) q"$value($index).toBoolean"
      else if (T =:= char || T =:= jChar) q"$value($index).head"
      else if (T =:= byte || T =:= jByte) q"$value($index).toByte"
      else if (T =:= short || T =:= jShort) q"$value($index).toShort"
      else if (T =:= int || T =:= jInt) q"$value($index).toInt"
      else if (T =:= long || T =:= jLong) q"$value($index).toLong"
      else if (T =:= float || T =:= jFloat) q"$value($index).toFloat"
      else if (T =:= double || T =:= jDouble) q"$value($index).toDouble"
      else if (T =:= bigInt) q"$Scala.BigInt($value($index))"
      else if (T =:= bigDec) q"$Scala.BigDecimal($value($index))"
      else if (T =:= jBigInt) q"new $Java.math.BigInteger($value($index))"
      else if (T =:= jBigDec) q"new $Java.math.BigDecimal($value($index))"
      else if (T =:= string) q"$value($index)"
      else q"{ $index; ${nil(T)} }"
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

      q"..$fields"
    } else {
      q"$builder += $value.toString"
    }
  }
}
