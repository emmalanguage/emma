package eu.stratosphere.emma.macros

import scala.reflect.macros.blackbox

// TODO: add DateTime support
class ConvertersMacros(val c: blackbox.Context) extends BlackBoxUtil {
  import universe._
  import syntax._

  val nextIndex = $"nextIndex"
  val builder = $"builder"

  /** Entry macro for emma algorithms. */
  def materializeCSVConverters[T: c.WeakTypeTag] = {
    val tpe = weakTypeOf[T]
    val $(v, i, sep) = $("value", "i", "separator")

    val fromStringFun = q"""
      def fromCSV($v: ${ARRAY(STRING)}): $tpe = {
        var $i = -1
        def $nextIndex = { $i += 1; $i }
        ${fromString[T](tpe, q"$v")}
      }"""

    val toStringFun = q"""
      def toCSV($v: $tpe, $sep: $CHAR): ${ARRAY(STRING)} = {
        $builder.clear()
        ${toString[T](tpe, q"$v")}
        $builder.result()
      }"""

    q"""new _root_.eu.stratosphere.emma.api.CSVConverters[$tpe] {
      val $builder = _root_.scala.collection.mutable.ArrayBuilder.make[$STRING]
      $fromStringFun
      $toStringFun
    }"""
  }

  def fromString[T: c.WeakTypeTag](tpe: Type, value: Tree): Tree =
    if (tpe <:< typeOf[Product]) {
      val method = tpe.decl(termNames.CONSTRUCTOR).alternatives.head.asMethod
      val params = method.typeSignatureIn(tpe).paramLists.head
      val args   = for (p <- params) yield fromString(p.info, value)
      q"new $tpe(..$args)"
    }

    else if (tpe =:= UNIT   || tpe =:= VOID)     q"()"
    else if (tpe =:= BOOL   || tpe =:= J_BOOL)   q"$value($nextIndex).toBoolean"
    else if (tpe =:= CHAR   || tpe =:= J_CHAR)   q"$value($nextIndex).head"
    else if (tpe =:= BYTE   || tpe =:= J_BYTE)   q"$value($nextIndex).toByte"
    else if (tpe =:= SHORT  || tpe =:= J_SHORT)  q"$value($nextIndex).toShort"
    else if (tpe =:= INT    || tpe =:= J_INT)    q"$value($nextIndex).toInt"
    else if (tpe =:= LONG   || tpe =:= J_LONG)   q"$value($nextIndex).toLong"
    else if (tpe =:= FLOAT  || tpe =:= J_FLOAT)  q"$value($nextIndex).toFloat"
    else if (tpe =:= DOUBLE || tpe =:= J_DOUBLE) q"$value($nextIndex).toDouble"
    else if (tpe =:= BIG_INT)   q"_root_.scala.BigInt($value($nextIndex))"
    else if (tpe =:= BIG_DEC)   q"_root_.scala.BigDecimal($value($nextIndex))"
    else if (tpe =:= J_BIG_INT) q"new _root_.java.math.BigInteger($value($nextIndex))"
    else if (tpe =:= J_BIG_DEC) q"new _root_.java.math.BigDecimal($value($nextIndex))"
    else if (tpe =:= STRING)    q"$value($nextIndex)"
    else q"{ $nextIndex; null.asInstanceOf[$tpe] }"

  def toString[T: c.WeakTypeTag](tpe: Type, value: Tree): Tree = {
    if (tpe <:< typeOf[Product]) {
      val method = tpe.decl(termNames.CONSTRUCTOR).alternatives.head.asMethod
      val params = method.typeSignatureIn(tpe).paramLists.head
      val fields = for {
        param  <- params
        method <- tpe.members
        if method.isMethod
        if method.asMethod.isGetter
        if method.toString == param.toString
      } yield toString(method.infoIn(tpe).resultType, q"$value.$method")

      q"..$fields"
    } else q"$builder += $value.toString"
  }
}
