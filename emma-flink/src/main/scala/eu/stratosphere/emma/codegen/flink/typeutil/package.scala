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
    } else if (tpe =:= weakTypeOf[Unit] || tpe =:= weakTypeOf[java.lang.Void]) {
      SimpleTypeConvertor.Unit
    } else if (tpe =:= weakTypeOf[Boolean] || tpe =:= weakTypeOf[java.lang.Boolean]) {
      SimpleTypeConvertor.Boolean
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
      throw new RuntimeException(s"Unsupported field type $tpe.")
    }
  }

  // ------------------------------------------------------------------------
  // Basic trait
  // ------------------------------------------------------------------------

  sealed trait TypeConvertor {
    val srcTpe: Type

    def tgtType: Tree

    def tgtTypeInfo: Tree

    def convertResultType(expr: Tree, expandSymbols: Set[Symbol] = Set.empty[Symbol]): Tree

    def convertTermType(termName: TermName, expr: Tree): Tree

    def srcToTgt(v: Tree): Tree

    def tgtToSrc(v: Tree): Tree
  }

  // ------------------------------------------------------------------------
  // Simple types
  // ------------------------------------------------------------------------

  final case class SimpleTypeConvertor(srcTpe: Type, javaClass: Tree) extends TypeConvertor {
    lazy val tgtType: Tree = tq"${TypeName(srcTpe.toString.split('.').last)}"

    lazy val tgtTypeInfo: Tree = q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$tgtType])"

    def convertResultType(expr: Tree, expandSymbols: Set[Symbol] = Set.empty[Symbol]) = expr

    def convertTermType(termName: TermName, expr: Tree) = expr

    def srcToTgt(v: Tree) = v

    def tgtToSrc(v: Tree) = v
  }

  object SimpleTypeConvertor {
    val Unit = SimpleTypeConvertor(typeOf[Unit], q"Unit.getClass")
    val Boolean = SimpleTypeConvertor(typeOf[Boolean], q"Boolean.getClass")
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
        case 9 => tq"org.apache.flink.api.java.tuple.Tuple9[..${fields.map(_.tgtType)}]"
        case 10 => tq"org.apache.flink.api.java.tuple.Tuple10[..${fields.map(_.tgtType)}]"
        case 11 => tq"org.apache.flink.api.java.tuple.Tuple11[..${fields.map(_.tgtType)}]"
        case 12 => tq"org.apache.flink.api.java.tuple.Tuple12[..${fields.map(_.tgtType)}]"
        case 13 => tq"org.apache.flink.api.java.tuple.Tuple13[..${fields.map(_.tgtType)}]"
        case 14 => tq"org.apache.flink.api.java.tuple.Tuple14[..${fields.map(_.tgtType)}]"
        case 15 => tq"org.apache.flink.api.java.tuple.Tuple15[..${fields.map(_.tgtType)}]"
        case 16 => tq"org.apache.flink.api.java.tuple.Tuple16[..${fields.map(_.tgtType)}]"
        case 17 => tq"org.apache.flink.api.java.tuple.Tuple17[..${fields.map(_.tgtType)}]"
        case 18 => tq"org.apache.flink.api.java.tuple.Tuple18[..${fields.map(_.tgtType)}]"
        case 19 => tq"org.apache.flink.api.java.tuple.Tuple19[..${fields.map(_.tgtType)}]"
        case 20 => tq"org.apache.flink.api.java.tuple.Tuple20[..${fields.map(_.tgtType)}]"
        case 21 => tq"org.apache.flink.api.java.tuple.Tuple21[..${fields.map(_.tgtType)}]"
        case _ => throw new RuntimeException(s"Too many fields in type $srcTpe. Up to 21 fields are currently supported.")
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

    override def convertResultType(expr: Tree, expandSymbols: Set[Symbol] = Set.empty[Symbol]) = new TypeConstructorsSubstituter(srcTpe, tgtType, expandSymbols).transform(expr)

    override def convertTermType(termName: TermName, expr: Tree) = new TypeProjectionsSubstituter(termName, this).transform(expr)

    override def srcToTgt(v: Tree) = q"new $tgtType(..${
      for (p <- paths) yield p.foldLeft[Tree](v)((prefix, s) => q"$prefix.$s")
    })"

    override def tgtToSrc(v: Tree) = tgtToSrc(v, new Counter)

    private def tgtToSrc(v: Tree, idx: Counter): Tree = q"new $srcTpe(..${
      for (c <- children) yield c match {
        case x: ProductTypeConvertor => x.tgtToSrc(v, idx)
        case x: SimpleTypeConvertor => q"$v.${TermName(s"f${idx.advance.get - 1}")}"
      }
    })"
  }

  // --------------------------------------------------------------------------
  // Type subsitutors
  // --------------------------------------------------------------------------

  private class TypeProjectionsSubstituter(termName: TermName, typeConvertor: ProductTypeConvertor) extends Transformer {
    override def transform(tree: Tree): Tree = {
      val path = (for (sp <- getSelectorPath(tree); if sp.ident.name == termName) yield typeConvertor.paths.collectFirst({
        case p if p == sp.path => p
      })).flatten

      path match {
        case Some(p) =>
          q"$termName.${TermName(s"f${typeConvertor.paths.indexOf(p)}")}"
        case _ =>
          super.transform(tree)
      }
    }
  }

  // FIXME: this is a quick and dirty solution that might break the UDF code
  private class TypeConstructorsSubstituter(srcTpe: Type, tgtTpe: Tree, expandSymbols: Set[Symbol]) extends Transformer {

    val applies = srcTpe.companion.member(TermName("apply")).alternatives
    val ctors = srcTpe.decl(termNames.CONSTRUCTOR).alternatives

    override def transform(tree: Tree): Tree = tree match {
      case Apply(fn, args) if applies.contains(fn.symbol) =>
        q"new $tgtTpe(..${expandArgs(args)})"
      case Apply(fn, args) if ctors.contains(fn.symbol) =>
        q"new $tgtTpe(..${expandArgs(args)})"
      case _ =>
        super.transform(tree)
    }

    /**
     * Expands the fields of selected product type symbols. For example, if the original arguments are
     *
     * {{{
     * (x, y._2, z)
     * }}}
     *
     * and the symbol of `y` is in the given set of `expandSymbols`, this utility function will substitute `y._2` with
     * its children. If `y._2` is of type `scala.Tuple3`, the resulting expanded list of arguments will look like:
     *
     * {{{
     * (x, y._2._1, y._2._2, y._2._3, z)
     * }}}
     *
     * where `y.fi` are the simple-typed fields of the tuple type that is used to encode the type of `y`.
     */
    def expandArgs(args: List[Tree]) = args.flatMap(a => {
      // check whether the argument is a selector path on one of the expanded symbols
      val p = getSelectorPath(a).filter(x => expandSymbols.contains(x.ident.symbol))

      if (p.isDefined) {
        // if this is the case, expand the subtree at this path
        createTypeConvertor(a.tpe).srcToTgt(a) match {
          case Apply(tgtFn, tgtArgs) =>
            tgtArgs // ProductTypeConverter: expand children
          case _ =>
            List(a) // SimpleTypeConverter: merely forward
        }
      } else {
        // Otherwise, merely forward
        List(a)
      }
    })
  }

  /**
   * Recusively constructs the `SelectorPath` located ending at the given `tree`.
   *
   * @param tree The current head of the path.
   * @param path The already visited ancestors
   * @return Optionally a `SelectorPath`, if the originally passed tree represents a valid `SelectorPath`.
   */
  def getSelectorPath(tree: Tree, path: List[Symbol] = List.empty[Symbol]): Option[SelectorPath] = tree match {
    case proj@Select(parent, _) => getSelectorPath(parent, List(proj.symbol) ++ path)
    case ident@Ident(TermName(_)) => Some(SelectorPath(ident, path))
    case _ => Option.empty[SelectorPath]
  }

  /**
   * Represents a selector chain of the form
   *
   * {{{
   * q"$ident.${path(0)}.${path(1)}...${path(n)}"
   * }}}
   *
   * @param ident The identifier at the beginning of the path.
   * @param path The symbols at the path select steps (in order).
   */
  case class SelectorPath(ident: Ident, path: List[Symbol])

}
