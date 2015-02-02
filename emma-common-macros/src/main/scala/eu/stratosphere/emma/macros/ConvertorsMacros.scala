package eu.stratosphere.emma.macros

import eu.stratosphere.emma.api.CSVConvertors

import scala.reflect.macros.blackbox

// TODO: reuse array lists
// TODO: add DateTime support
class ConvertorsMacros(val c: blackbox.Context) {

  import c.universe._

  /**
   * Entry macro for emma algorithms.
   */
  def materializeCSVConvertorsImpl[T: c.WeakTypeTag]: c.Expr[CSVConvertors[T]] = {
    val tpe = weakTypeOf[T]

    val fromStringFn = q"""
      def fromCSV(value: Array[String]): $tpe = {

        var i = -1;

        def nextIndex = { i = i + 1; i }

        ${fromString[T](tpe, q"value")}
      }"""

    val toStringFn = q"""
      def toCSV(value: $tpe, separator: Char): Array[String] = {

        builder.clear()

        ${toString[T](tpe, q"value")}

        builder.result()
      }"""

    c.Expr[CSVConvertors[T]](
      q"""
       new eu.stratosphere.emma.api.CSVConvertors[$tpe] {

         import scala.collection.mutable.ArrayBuilder

         val builder = ArrayBuilder.make[String]()

         $fromStringFn

         $toStringFn
       }
       """
    )
  }

  def fromString[T: c.WeakTypeTag](tpe: Type, value: Tree): Tree = {

    if (tpe <:< weakTypeOf[Product]) {
      val params = tpe.decl(termNames.CONSTRUCTOR).alternatives.head.asMethod.typeSignatureIn(tpe).paramLists.head
      val args = params.map(arg => fromString(arg.info, value))
      q"new $tpe(..$args)"
    } else if (tpe =:= weakTypeOf[Unit] || tpe =:= weakTypeOf[java.lang.Void]) {
      q"Unit"
    } else if (tpe =:= weakTypeOf[Boolean] || tpe =:= weakTypeOf[java.lang.Boolean]) {
      q"$value(nextIndex).toBoolean"
    } else if (tpe =:= weakTypeOf[Byte]) {
      q"$value(nextIndex).toByte"
    } else if (tpe =:= weakTypeOf[Short] || tpe =:= weakTypeOf[java.lang.Short]) {
      q"$value(nextIndex).toShort"
    } else if (tpe =:= weakTypeOf[Short] || tpe =:= weakTypeOf[java.lang.Short]) {
      q"$value(nextIndex).toShort"
    } else if (tpe =:= weakTypeOf[Int] || tpe =:= weakTypeOf[java.lang.Integer]) {
      q"$value(nextIndex).toInt"
    } else if (tpe =:= weakTypeOf[Long] || tpe =:= weakTypeOf[java.lang.Long]) {
      q"$value(nextIndex).toLong"
    } else if (tpe =:= weakTypeOf[Float] || tpe =:= weakTypeOf[java.lang.Float]) {
      q"$value(nextIndex).toFloat"
    } else if (tpe =:= weakTypeOf[Double] || tpe =:= weakTypeOf[java.lang.Double]) {
      q"$value(nextIndex).toDouble"
    } else if (tpe =:= weakTypeOf[String] || tpe =:= weakTypeOf[java.lang.String]) {
      q"$value(nextIndex)"
    } else {
      // c.warning(c.enclosingPosition, s"Cannot synthesize 'fromString' method for type ${tpe.toString}") // TODO: enable warning
      q"{ nextIndex; null.asInstanceOf[$tpe] }"
    }
  }

  def toString[T: c.WeakTypeTag](tpe: Type, value: Tree): Tree = {
    if (tpe <:< weakTypeOf[Product]) {
      val params = tpe.decl(termNames.CONSTRUCTOR).alternatives.head.asMethod.typeSignatureIn(tpe).paramLists.head.map(_.toString)
      val fields = for (p <- params; m <- tpe.members if m.isMethod && m.asMethod.isGetter && m.toString == p.toString) yield m
      val strngs = fields.map(arg => toString(arg.infoIn(tpe).resultType, q"$value.$arg"))
      q"..$strngs"
    } else if (tpe =:= weakTypeOf[Boolean] || tpe =:= weakTypeOf[java.lang.Boolean]) {
      q"builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Byte]) {
      q"builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Short] || tpe =:= weakTypeOf[java.lang.Short]) {
      q"builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Short] || tpe =:= weakTypeOf[java.lang.Short]) {
      q"builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Int] || tpe =:= weakTypeOf[java.lang.Integer]) {
      q"builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Long] || tpe =:= weakTypeOf[java.lang.Long]) {
      q"builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Float] || tpe =:= weakTypeOf[java.lang.Float]) {
      q"builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Double] || tpe =:= weakTypeOf[java.lang.Double]) {
      q"builder += $value.toString"
    } else if (tpe =:= weakTypeOf[String] || tpe =:= weakTypeOf[java.lang.String]) {
      q"builder += $value"
    } else {
      // c.warning(c.enclosingPosition, s"Cannot synthesize 'toString' method for type ${tpe.toString}") // TODO: enable warning
      EmptyTree
    }
  }
}
