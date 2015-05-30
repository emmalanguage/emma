package eu.stratosphere.emma.macros

import scala.reflect.macros.blackbox

// TODO: reuse array lists
// TODO: add DateTime support
class ConvertorsMacros(val c: blackbox.Context) extends BlackBoxUtil {
  import c.universe._
  val nextIndex = freshName("nextIndex$")
  val builder   = freshName("builder$")

  /** Entry macro for emma algorithms. */
  def materializeCSVConvertorsImpl[T: c.WeakTypeTag] = {
    val tpe = weakTypeOf[T]
    val as  = typeOf[Array[String]]
    val v   = freshName("value$")
    val i   = freshName("i$")
    val sep = freshName("separator$")

    val fromStringFn = q"""
      def fromCSV($v: $as): $tpe = {
        var $i = -1;
        def $nextIndex = { $i += 1; $i }
        ${fromString[T](tpe, q"$v")}
      }"""

    val toStringFn = q"""
      def toCSV($v: $tpe, $sep: ${typeOf[Char]}): $as = {
        $builder.clear()
        ${toString[T](tpe, q"$v")}
        $builder.result()
      }"""

    q"""new _root_.eu.stratosphere.emma.api.CSVConvertors[$tpe] {
      val $builder =
        _root_.scala.collection.mutable.ArrayBuilder.make[${typeOf[String]}]

      $fromStringFn
      $toStringFn
    }"""
  }

  def fromString[T: c.WeakTypeTag](tpe: Type, value: Tree): Tree =
    if (tpe <:< weakTypeOf[Product]) {
      val method = tpe.decl(termNames.CONSTRUCTOR).alternatives.head.asMethod
      val params = method.typeSignatureIn(tpe).paramLists.head
      val args   = for (p <- params) yield fromString(p.info, value)
      q"new $tpe(..$args)"
    } else if (tpe =:= weakTypeOf[Unit] ||
               tpe =:= weakTypeOf[java.lang.Void]) {
      q"()"
    } else if (tpe =:= weakTypeOf[Boolean] ||
               tpe =:= weakTypeOf[java.lang.Boolean]) {
      q"$value($nextIndex).toBoolean"
    } else if (tpe =:= weakTypeOf[Byte]) {
      q"$value($nextIndex).toByte"
    } else if (tpe =:= weakTypeOf[Short] ||
               tpe =:= weakTypeOf[java.lang.Short]) {
      q"$value($nextIndex).toShort"
    } else if (tpe =:= weakTypeOf[Short] ||
               tpe =:= weakTypeOf[java.lang.Short]) {
      q"$value($nextIndex).toShort"
    } else if (tpe =:= weakTypeOf[Int] ||
               tpe =:= weakTypeOf[java.lang.Integer]) {
      q"$value($nextIndex).toInt"
    } else if (tpe =:= weakTypeOf[Long] ||
               tpe =:= weakTypeOf[java.lang.Long]) {
      q"$value($nextIndex).toLong"
    } else if (tpe =:= weakTypeOf[Float] ||
               tpe =:= weakTypeOf[java.lang.Float]) {
      q"$value($nextIndex).toFloat"
    } else if (tpe =:= weakTypeOf[Double] ||
               tpe =:= weakTypeOf[java.lang.Double]) {
      q"$value($nextIndex).toDouble"
    } else if (tpe =:= weakTypeOf[String] ||
               tpe =:= weakTypeOf[java.lang.String]) {
      q"$value($nextIndex)"
    } else {
      q"{ $nextIndex; null.asInstanceOf[$tpe] }"
    }

  def toString[T: c.WeakTypeTag](tpe: Type, value: Tree): Tree = {
    if (tpe <:< weakTypeOf[Product]) {
      val method = tpe.decl(termNames.CONSTRUCTOR).alternatives.head.asMethod
      val params = method.typeSignatureIn(tpe).paramLists.head
      val fields = for {
        p <- params
        m <- tpe.members
        if m.isMethod
        if m.asMethod.isGetter
        if m.toString == p.toString
      } yield toString(m.infoIn(tpe).resultType, q"$value.$m")

      q"..$fields"
    } else if (tpe =:= weakTypeOf[Boolean] ||
               tpe =:= weakTypeOf[java.lang.Boolean]) {
      q"$builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Byte]) {
      q"$builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Short] ||
               tpe =:= weakTypeOf[java.lang.Short]) {
      q"$builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Short] ||
               tpe =:= weakTypeOf[java.lang.Short]) {
      q"$builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Int] ||
               tpe =:= weakTypeOf[java.lang.Integer]) {
      q"$builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Long] ||
               tpe =:= weakTypeOf[java.lang.Long]) {
      q"$builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Float] ||
               tpe =:= weakTypeOf[java.lang.Float]) {
      q"$builder += $value.toString"
    } else if (tpe =:= weakTypeOf[Double] ||
               tpe =:= weakTypeOf[java.lang.Double]) {
      q"$builder += $value.toString"
    } else if (tpe =:= weakTypeOf[String] ||
               tpe =:= weakTypeOf[java.lang.String]) {
      q"$builder += $value"
    } else {
      EmptyTree
    }
  }
}
