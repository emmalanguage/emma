package eu.stratosphere.emma.codegen.flink

import eu.stratosphere.emma.util.Counter

import scala.reflect.runtime.universe._

package object typeutil {

  // ------------------------------------------------------------------------
  // Factory
  // ------------------------------------------------------------------------

  /**
   * Type convertor factory.
   *
   * @param tpe The source type.
   * @return A type convertor for the given source type.
   */
  def createTypeConvertor(tpe: Type): TypeConvertor = {
    if (tpe <:< weakTypeOf[Product]) {
      val params = tpe.decl(termNames.CONSTRUCTOR).alternatives.head.asMethod.typeSignatureIn(tpe).paramLists.head.map(_.toString)
      val getters = for (p <- params; m <- tpe.members if m.isMethod && m.asMethod.isGetter && m.toString == p.toString) yield m
      new ProductTypeConvertor(tpe, getters)
    } else if (tpe =:= weakTypeOf[Byte] || tpe =:= weakTypeOf[java.lang.Byte]) {
      SimpleTypeConvertor.Byte
    } else if (tpe =:= weakTypeOf[Char] || tpe =:= weakTypeOf[java.lang.Character]) {
      SimpleTypeConvertor.Char
    } else if (tpe =:= weakTypeOf[Short] || tpe =:= weakTypeOf[java.lang.Short]) {
      SimpleTypeConvertor.Short
    } else if (tpe =:= weakTypeOf[Int] || tpe =:= weakTypeOf[java.lang.Integer]) {
      SimpleTypeConvertor.Int
    } else if (tpe =:= weakTypeOf[Long] || tpe =:= weakTypeOf[java.lang.Long]) {
      SimpleTypeConvertor.Long
    } else if (tpe =:= weakTypeOf[Float] || tpe =:= weakTypeOf[java.lang.Float]) {
      SimpleTypeConvertor.Float
    } else if (tpe =:= weakTypeOf[Double] || tpe =:= weakTypeOf[java.lang.Double]) {
      SimpleTypeConvertor.Double
    } else if (tpe =:= weakTypeOf[String] || tpe =:= weakTypeOf[java.lang.String]) {
      SimpleTypeConvertor.String
    } else {
      throw new RuntimeException("Too many distinct fields in type. Up to 8 fields are currently supported.")
    }
  }

  // ------------------------------------------------------------------------
  // Basic trait
  // ------------------------------------------------------------------------

  sealed trait TypeConvertor {
    val srcTpe: Type

    def tgtType: Tree

    def tgtTypeInfo: Tree

    def convertResultType(expr: Tree): Tree

    def convertTermType(termName: TermName, expr: Tree): Tree

    def srcToTgt(v: TermName): Tree

    def tgtToSrc(v: TermName): Tree
  }

  // ------------------------------------------------------------------------
  // Simple types
  // ------------------------------------------------------------------------

  final case class SimpleTypeConvertor(srcTpe: Type, javaClass: Tree) extends TypeConvertor {
    lazy val tgtType: Tree = tq"${TypeName(srcTpe.toString.split('.').last)}"

    lazy val tgtTypeInfo: Tree = q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$tgtType])"

    def convertResultType(expr: Tree) = expr

    def convertTermType(termName: TermName, expr: Tree) = expr

    def srcToTgt(v: TermName) = q"$v"

    def tgtToSrc(v: TermName) = q"$v"
  }

  object SimpleTypeConvertor {
    val Byte = SimpleTypeConvertor(typeOf[Byte], q"Byte.getClass")
    val Short = SimpleTypeConvertor(typeOf[Short], q"Short.getClass")
    val Char = SimpleTypeConvertor(typeOf[Char], q"Char.getClass")
    val Int = SimpleTypeConvertor(typeOf[Int], q"Int.getClass")
    val Long = SimpleTypeConvertor(typeOf[Long], q"Long.getClass")
    val Float = SimpleTypeConvertor(typeOf[Float], q"Float.getClass")
    val Double = SimpleTypeConvertor(typeOf[Double], q"Double.getClass")
    val String = SimpleTypeConvertor(typeOf[String], q"classOf[String]")
  }

  final case class ProductTypeConvertor private[typeutil](srcTpe: Type, projections: List[Symbol] = List.empty[Symbol]) extends TypeConvertor {

    override val tgtType = {
      fields.size match {
        case 0 => tq"Unit"
        case 1 => tq"org.apache.flink.api.java.tuple.Tuple1[..${fields.map(_.tgtType)}]"
        case 2 => tq"org.apache.flink.api.java.tuple.Tuple2[..${fields.map(_.tgtType)}]"
        case 3 => tq"org.apache.flink.api.java.tuple.Tuple3[..${fields.map(_.tgtType)}]"
        case 4 => tq"org.apache.flink.api.java.tuple.Tuple4[..${fields.map(_.tgtType)}]"
        case 5 => tq"org.apache.flink.api.java.tuple.Tuple5[..${fields.map(_.tgtType)}]"
        case 6 => tq"org.apache.flink.api.java.tuple.Tuple6[..${fields.map(_.tgtType)}]"
        case 7 => tq"org.apache.flink.api.java.tuple.Tuple7[..${fields.map(_.tgtType)}]"
        case 8 => tq"org.apache.flink.api.java.tuple.Tuple8[..${fields.map(_.tgtType)}]"
        case _ => throw new RuntimeException(s"Too many fields in type $srcTpe. Up to 8 fields are currently supported.")
      }
    }

    override val tgtTypeInfo =
      q"""
      new org.apache.flink.api.java.typeutils.TupleTypeInfo[$tgtType](
       Array[org.apache.flink.api.common.typeinfo.TypeInformation[_]](..${fields.map(_.tgtTypeInfo)}))
      """

    lazy val children = for (p <- projections) yield createTypeConvertor(p.typeSignatureIn(srcTpe).finalResultType.dealias)

    lazy val fields: List[SimpleTypeConvertor] = (for (c <- children) yield c match {
      case x: ProductTypeConvertor => x.fields
      case x: SimpleTypeConvertor => List(x)
    }).flatten

    lazy val paths: List[List[Symbol]] = (for ((p, c) <- projections zip children) yield c match {
      case x: ProductTypeConvertor =>
        val z = for (suffix <- x.paths) yield p :: suffix
        z
      case _: SimpleTypeConvertor =>
        val z = List(List[Symbol](p))
        z
    }).flatten

    override def convertResultType(expr: Tree) = new TypeConstructorsSubstituter(srcTpe, tgtType).transform(expr)

    override def convertTermType(termName: TermName, expr: Tree) = new TypeProjectionsSubstituter(termName, this).transform(expr)

    override def srcToTgt(v: TermName) = q"new $tgtType(..${
      for (p <- paths) yield p.foldLeft[Tree](q"$v")((prefix, s) => q"$prefix.$s")
    })"

    override def tgtToSrc(v: TermName) = tgtToSrc(v, new Counter)

    private def tgtToSrc(v: TermName, idx: Counter): Tree = q"new $srcTpe(..${
      for (c <- children) yield c match {
        case x: ProductTypeConvertor => x.tgtToSrc(v, idx)
        case x: SimpleTypeConvertor => q"$v.${TermName(s"f${idx.advance.get - 1}")}"
      }
    })"
  }

  // --------------------------------------------------------------------------
  // Type subsitutors
  // --------------------------------------------------------------------------

  // FIXME: this is a quick and dirty solution that only works for selector chains with limited depth
  private class TypeProjectionsSubstituter(termName: TermName, typeConvertor: ProductTypeConvertor) extends Transformer {

    override def transform(tree: Tree): Tree = {
      val path = tree match {
        case p3@Select(p2@Select(p1@Select(Ident(x), _), _), _) if x == termName =>
          typeConvertor.paths.collectFirst({
            case p if p == List(p1.symbol, p2.symbol, p3.symbol) => p
          })
        case p2@Select(p1@Select(Ident(x), _), _) if x == termName =>
          typeConvertor.paths.collectFirst({
            case p if p == List(p1.symbol, p2.symbol) => p
          })
        case p1@Select(Ident(x), _) if x == termName =>
          typeConvertor.paths.collectFirst({
            case p if p == List(p1.symbol) => p
          })
        case _ => Option.empty[List[Symbol]]
      }

      path match {
        case Some(p) =>
          q"$termName.${TermName(s"f${typeConvertor.paths.indexOf(p)}")}"
        case _ =>
          super.transform(tree)
      }
    }
  }

  // FIXME: this is a quick and dirty solution that might break the UDF code
  private class TypeConstructorsSubstituter(srcTpe: Type, tgtTpe: Tree) extends Transformer {

    val applies = srcTpe.companion.member(TermName("apply")).alternatives
    val ctors = srcTpe.decl(termNames.CONSTRUCTOR).alternatives

    override def transform(tree: Tree): Tree = tree match {
      case Apply(fn, args) if applies.contains(fn.symbol) =>
        q"new $tgtTpe(..$args)"
      case Apply(fn, args) if ctors.contains(fn.symbol) =>
        q"new $tgtTpe(..$args)"
      case _ =>
        super.transform(tree)
    }
  }

}
