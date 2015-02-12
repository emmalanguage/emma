package eu.stratosphere.emma.codegen.flink.typeutil

import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.util.Counter

import scala.collection.mutable
import scala.reflect.runtime.universe._

class TypeInformationFactory(val dataflowCompiler: DataflowCompiler) {

  import eu.stratosphere.emma.runtime.logger

  val counter = new Counter

  val tb = dataflowCompiler.tb

  // memoization table for synthesized TypeInformation instances
  val memo = mutable.Map[Type, ClassSymbol]()

  def apply(tpe: Type): Tree = {
    val widenedTpe = tpe.widen

    if (widenedTpe <:< weakTypeOf[Product]) {
      productTypeInformationFor(tpe)
    } else if (widenedTpe =:= weakTypeOf[Unit] || widenedTpe =:= weakTypeOf[java.lang.Void]) {
      q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$widenedTpe])"
    } else if (widenedTpe =:= weakTypeOf[Boolean] || widenedTpe =:= weakTypeOf[java.lang.Boolean]) {
      q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$widenedTpe])"
    } else if (widenedTpe =:= weakTypeOf[Byte] || widenedTpe =:= weakTypeOf[java.lang.Byte]) {
      q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$widenedTpe])"
    } else if (widenedTpe =:= weakTypeOf[Char] || widenedTpe =:= weakTypeOf[java.lang.Character]) {
      q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$widenedTpe])"
    } else if (widenedTpe =:= weakTypeOf[Short] || widenedTpe =:= weakTypeOf[java.lang.Short]) {
      q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$widenedTpe])"
    } else if (widenedTpe =:= weakTypeOf[Int] || widenedTpe =:= weakTypeOf[java.lang.Integer]) {
      q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$widenedTpe])"
    } else if (widenedTpe =:= weakTypeOf[Long] || widenedTpe =:= weakTypeOf[java.lang.Long]) {
      q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$widenedTpe])"
    } else if (widenedTpe =:= weakTypeOf[Float] || widenedTpe =:= weakTypeOf[java.lang.Float]) {
      q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$widenedTpe])"
    } else if (widenedTpe =:= weakTypeOf[Double] || widenedTpe =:= weakTypeOf[java.lang.Double]) {
      q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$widenedTpe])"
    } else if (widenedTpe =:= weakTypeOf[String] || widenedTpe =:= weakTypeOf[java.lang.String]) {
      q"org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor(classOf[$widenedTpe])"
    } else {
      throw new RuntimeException(s"Unsupported field type $widenedTpe.")
    }
  }

  private def productTypeInformationFor(tpe: Type): Tree = {
    val symbol = memo.getOrElse(tpe, {
      val params = tpe.decl(termNames.CONSTRUCTOR).alternatives.head.asMethod.typeSignatureIn(tpe).paramLists.head.map(_.toString)
      val fields = for (p <- params; m <- tpe.members if m.isMethod && m.asMethod.isGetter && m.toString == p.toString) yield m

      val fieldNames = for (field <- fields) yield field.name.toString
      val fieldTypes = for (field <- fields) yield field.infoIn(tpe).resultType
      val fieldTypeInfos = for (fieldType <- fieldTypes) yield apply(fieldType)

      val name = f"SynthesizedTypeInformation${counter.advance.get}%03d"
      logger.info(s"Synthesizing type information for '$tpe' in class '$name'")

      val tree =
        q"""
        class ${TypeName(name)} extends eu.stratosphere.emma.codegen.flink.typeutil.CaseClassTypeInfo[$tpe](classOf[$tpe], Seq(..$fieldTypeInfos), Seq(..$fieldNames)) {

          import org.apache.flink.api.common.typeutils.TypeSerializer
          import eu.stratosphere.emma.codegen.flink.typeutil.CaseClassSerializer

          override def createSerializer: TypeSerializer[$tpe] = {
            val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
            for (i <- 0 until getArity) {
              fieldSerializers(i) = types(i).createSerializer
            }

            new CaseClassSerializer[$tpe](tupleType, fieldSerializers) {
              override def createInstance(fields: Array[AnyRef]): $tpe = {
                new $tpe(..${for ((t, i) <- fieldTypes.zipWithIndex) yield q"fields($i).asInstanceOf[$t]"})
              }
            }
          }
        }
        """.asInstanceOf[ClassDef]

      val symbol = dataflowCompiler.compile(tree).asClass
      logger.debug(s"Synthesized type information for '$tpe' in class '${symbol.fullName}'")

      memo.put(tpe, symbol)
      symbol
    })

    q"new $symbol"
  }
}