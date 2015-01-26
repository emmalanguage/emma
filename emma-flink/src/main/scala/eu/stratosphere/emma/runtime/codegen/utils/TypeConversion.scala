package eu.stratosphere.emma.runtime.codegen.utils

/**
 *
 */
object TypeConversion {

  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{currentMirror => cm}

//  def typeInformation(ref: Type): TypeInformation = {
//    ref match {
//      case TypeRef(pre, sym, args) =>
//        try {
//          sym match {
//            case p if definitions.ScalaPrimitiveValueClasses.contains(p) =>
//              val cSym = cm.runtimeClass(sym.asClass)
//              new TypeInformation(cSym)
//            case t if definitions.TupleClass.seq.contains(t) =>
//              throw new UnsupportedOperationException("scala tuples currently not supported.")
//            // Flink tuples
//            case a if ref <:< typeOf[AbstractTuple] =>
//              val children = for (arg <- args) yield typeInformation(arg)
//              val cSym = cm.runtimeClass(sym.asClass)
//              new TypeInformation(cSym, children: _*)
//            case _ => // String refs usw...
//              new TypeInformation(cm.runtimeClass(ref))
//          }
//          // // TODO: make const strings work!
//        } catch {
//          case e: Exception => throw new Exception(s"pre: $pre > sym: $sym > args: $args > ref: $ref")
//        }
//      case t@ConstantType(a) =>
//        println(a)
//        new TypeInformation(a.getClass)
//      case a => throw new UnsupportedOperationException()
//    }
//  }

}
