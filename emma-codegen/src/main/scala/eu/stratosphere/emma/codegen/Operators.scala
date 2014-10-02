//package eu.stratosphere.emma.codegen
//
//import de.tuberlin.aura.core.record.TypeInformation
//
///**
// *
// */
//object Operators {
//
//  import scala.reflect.runtime.universe._
//  import scala.reflect.runtime.{currentMirror => cm}
//  import scala.tools.reflect.ToolBox
//
//  val mk = cm.mkToolBox()
//
//  case class MapNode(var in: Expr[Any], var func: Expr[Any]) {
//
//    def udf = {
//      val inputParam = toParam(in)
//      val inputType = in.staticType.typeSymbol
//      val outputType = func.staticType.typeSymbol
//
//      val trans = new Transformer {
//
//        override def transform(tree: Tree): Tree = tree match {
//          case ident@Ident(name: TermName) => Ident(TermName(name.toString))
//          case _ => super.transform(tree)
//        }
//      }
//
//      val body = trans.transform(func.tree)
//
//      val name = newName("Mapper")
//
//      val inputTypeInformation = typeInformation(in.staticType.asInstanceOf[TypeRef])
//      val outputTypeInfromation = typeInformation(func.staticType.asInstanceOf[TypeRef])
//
//      //      val tree = q"""
//      //      final class $name extends _root_.de.tuberlin.aura.core.processing.udfs.functions.MapFunction[$inputType, $outputType] {
//      //        override def map($inputParam): $outputType = {
//      //          ..$body
//      //        }
//      //      }
//      //      """
//      //
//      //      val properties =
//      //        new DataflowNodeProperties(
//      //          UUID.randomUUID,
//      //          DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
//      //          1,
//      //          null,
//      //          Partitioner.PartitioningStrategy.ROUND_ROBIN_PARTITIONER,
//      //          4,
//      //          "Map1",
//      //          inputTypeInformation,
//      //          null,
//      //          outputTypeInfromation,
//      //          classOf[MapFunction[inputType.type, outputType.type]],
//      //          null,
//      //          null,
//      //          null,
//      //          null,
//      //          null
//      //        )
//      //
//      //      properties
//    }
//  }
//
//
//  /**
//   * Introduce support for generic classes and tuple types
//   *
//   * @param ref
//   * @return
//   */
//  def typeInformation(ref: Type): TypeInformation = {
//    ref match {
//      case TypeRef(pre, sym, args) =>
//        println(pre)
//        println(sym)
//        println(args)
//        println("-------")
//        sym match {
//          case p if definitions.ScalaPrimitiveValueClasses.contains(p) =>
//            val cSym = cm.runtimeClass(sym.asClass)
//            new TypeInformation(cSym)
//          case t if definitions.TupleClass.seq.contains(t) =>
//            val children = for (arg <- args) yield typeInformation(arg)
//            //            new TypeInformation(cm.runtimeClass(sym.asClass), JavaConversions.bufferAsJavaList(ListBuffer(children: _*)))
//            new TypeInformation(cm.runtimeClass(sym.asClass))
//          case _ => throw new UnsupportedOperationException("")
//        }
//      case a => throw new UnsupportedOperationException("Only TypeRefs supported currently.")
//    }
//  }
//}
