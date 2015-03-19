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
      q"new org.apache.flink.api.java.typeutils.GenericTypeInfo[$widenedTpe](classOf[$widenedTpe])"
    }
  }

  private def dataBagTypeInformationFor(tpe: Type): Tree = {
    val symbol = memo.getOrElse(tpe, {
      val collectionClass = q"classOf[eu.stratosphere.emma.api.DataBag]"
      val elementClazz = q""
      val elementTypeInfo = apply(tq"".tpe)

//      val cbf = q"implicitly[CanBuildFrom[${desc.tpe}, ${desc.elem.tpe}, ${desc.tpe}]]"

      val tree = q"".asInstanceOf[ClassDef]
//        q"""
//        import scala.collection.generic.CanBuildFrom
//        import org.apache.flink.api.scala.typeutils.TraversableTypeInfo
//        import org.apache.flink.api.scala.typeutils.TraversableSerializer
//        import org.apache.flink.api.common.ExecutionConfig
//
//        val elementTpe = $elementTypeInfo
//        new TraversableTypeInfo($collectionClass, elementTpe) {
//          def createSerializer(executionConfig: ExecutionConfig) = {
//            new TraversableSerializer[${desc.tpe}, ${desc.elem.tpe}](
//                elementTpe.createSerializer(executionConfig)) {
//              def getCbf = implicitly[CanBuildFrom[${desc.tpe}, ${desc.elem.tpe}, ${desc.tpe}]]
//            }
//          }
//        }
//      """.asInstanceOf[ClassDef]

      val symbol = dataflowCompiler.compile(tree).asClass
      logger.debug(s"Synthesized type information for '$tpe' in class '${symbol.fullName}'")

      memo.put(tpe, symbol)
      symbol
    })

    q"new $symbol"
  }

  private def productTypeInformationFor(tpe: Type): Tree = {
    val symbol = memo.getOrElse(tpe, {
      val params = tpe.decl(termNames.CONSTRUCTOR).alternatives.head.asMethod.typeSignatureIn(tpe).paramLists.head.map(_.toString)
      val fields = for (p <- params; m <- tpe.members if m.isMethod && m.asMethod.isGetter && m.toString == p.toString) yield m

      val fieldNames = for (field <- fields) yield field.name.toString
      val fieldTypes = for (field <- fields) yield field.infoIn(tpe).resultType
      val fieldTypeInfos = for (fieldType <- fieldTypes) yield apply(fieldType)
      val genericTypeInfos = tpe match {
        case TypeRef(_, _, typeParams) =>
          typeParams map { genericTpe => apply(genericTpe) }
        case _ =>
          List.empty[Tree]
      }

      val name = f"SynthesizedTypeInformation${counter.advance.get}%03d"
      logger.info(s"Synthesizing type information for '$tpe' in class '$name'")

      val tree =
        q"""
        class ${TypeName(name)} extends org.apache.flink.api.scala.typeutils.CaseClassTypeInfo[$tpe](classOf[$tpe], Array[org.apache.flink.api.common.typeinfo.TypeInformation[_]](..$genericTypeInfos), Seq(..$fieldTypeInfos), Seq(..$fieldNames)) {

          import org.apache.flink.api.common.typeutils.TypeSerializer
          import org.apache.flink.api.scala.typeutils.CaseClassSerializer
          import org.apache.flink.api.common.ExecutionConfig

          override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[$tpe] = {
            val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
            for (i <- 0 until getArity) {
              fieldSerializers(i) = types(i).createSerializer(executionConfig)
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