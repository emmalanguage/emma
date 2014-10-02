//package eu.stratosphere.emma.codegen
//
//import java.util.UUID
//
//import _root_.eu.stratosphere.emma.ir._
//import de.tuberlin.aura.client.api.AuraClient
//import de.tuberlin.aura.client.executors.LocalClusterSimulator
//import de.tuberlin.aura.core.config.{IConfig, IConfigFactory}
//import de.tuberlin.aura.core.dataflow.generator.TopologyGenerator
//import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowAPI.DataflowNodeDescriptor
//import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowNodeProperties
//import de.tuberlin.aura.core.dataflow.udfs.functions.{MapFunction, SinkFunction, SourceFunction}
//import de.tuberlin.aura.core.record.{Partitioner, TypeInformation}
//import de.tuberlin.aura.core.topology.Topology
//import eu.stratosphere.emma.generate.aura.Utils._
//
//import scala.collection.JavaConversions
//import scala.collection.mutable.ListBuffer
//import scala.tools.reflect.FrontEnd
//
//// conversions to java
//
//class Generate {
//
//  import scala.reflect.runtime.universe._
//  import scala.reflect.runtime.{currentMirror => cm}
//  import scala.tools.reflect.ToolBox
//
//  val generatedClassDir = "/home/akunft/generated" //Files.createTempDirectory("generated")
//
//  val mk = cm.mkToolBox(
//    new FrontEnd {
//      def display(info: Info) {}
//
//      def interactive() {}
//    },
//    s"-d ${generatedClassDir}"
//  )
//
//  //  def compile(df: Dataflow): Unit = {
//  //    for (s <- df.sinks) {
//  //
//  //      val builder = List.newBuilder[Tree]
//  //
//  //      for (e <- sequence(s)) {
//  //        e match {
//  //          case FilterCombinator(Filter(ScalaExpr(f, p)), qs) =>
//  //            builder += buildFilter(f, p, qs)
//  //          case MapCombinator(ScalaExpr(f, p), i) =>
//  //            if (f.foldRight(false)((a, b) => a._1.startsWith("src$record$") || b)) {
//  //              builder += buildSource(f, p, i)
//  //            } else if (f.foldRight(false)((a, b) => a._1.contains("snk$record$") || b)) {
//  //              builder += buildSink(f, p, i)
//  //            } else
//  //              builder += buildMap(f, p, i)
//  //          case _ =>
//  //        }
//  //      }
//  //
//  //      saveToFile("/home/akunft/generated/all.scala", embbedExecutable(builder.result()))
//  //    }
//  //  }
//
//  def compile(df: Dataflow): Unit = {
//    for (s <- df.sinks) {
//
//      val builder = List.newBuilder[DataflowNodeProperties]
//
//      for (e <- sequence(s)) {
//        e match {
//          case FilterCombinator(Filter(ScalaExpr(f, p)), qs) =>
//            builder += buildFilter(f, p, qs)
//          case MapCombinator(ScalaExpr(f, p), i) =>
//            if (f.foldRight(false)((a, b) => a._1.startsWith("src$record$") || b)) {
//              println("source")
//              builder += buildSource(f, p, i)
//            } else if (f.foldRight(false)((a, b) => a._1.contains("snk$record$") || b)) {
//              println("sink")
//              builder += buildSink(f, p, i)
//            } else {
//              println("map")
//              builder += buildMap(f, p, i)
//            }
//          case _ =>
//        }
//      }
//
//      createPlan(builder.result())
//
//      //saveToFile("/home/akunft/generated/all.scala", embbedExecutable(builder.result()))
//    }
//  }
//
//  def createPlan(props: List[DataflowNodeProperties]): Unit = {
//
//    var sink: DataflowNodeDescriptor = null
//
//    for (p <- props) {
//      if (sink == null) {
//        sink = new DataflowNodeDescriptor(p)
//      } else {
//        sink = new DataflowNodeDescriptor(p, sink)
//      }
//    }
//
//    println(sink)
//
//    val lcs: LocalClusterSimulator = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR))
//    val ac: AuraClient = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT))
//
//    val topology1: Topology.AuraTopology = new TopologyGenerator(ac.createTopologyBuilder).generate(sink).toTopology("JOB1")
//    ac.submitTopology(topology1, null)
//  }
//
//  def buildSource(f: Map[String, Expr[Any]], p: Expr[Any], qualifier: Qualifier): DataflowNodeProperties = {
//    val outputType = p.staticType.typeSymbol
//
//    val trans = new Transformer {
//      override def transform(tree: Tree): Tree = tree match {
//        case ident@Ident(name: TermName) => Ident(TermName(name.toString))
//        case _ => super.transform(tree)
//      }
//    }
//
//    val body = trans.transform(p.tree)
//
//    val name = newName("Source")
//
//    val outputTypeInformation = typeInformation(p.staticType.asInstanceOf[TypeRef])
//
//    val classTree = q"""
//      class $name extends de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction[de.tuberlin.aura.core.record.tuples.Tuple2[Int, Int]] {
//        var counter: Int = 1000
//
//        override def produce(): de.tuberlin.aura.core.record.tuples.Tuple2[Int, Int] = {
//          counter = counter - 1
//          if (counter > 0) new de.tuberlin.aura.core.record.tuples.Tuple2[Int, Int](counter, 1) else null
//        }
//      }
//    """
//
//    val classSymbol = mk.define(classTree.asInstanceOf[ClassDef])
//
//    val clazz = this.getClass.getClassLoader.loadClass(classSymbol.owner.fullName + s".$name").asInstanceOf[Class[SourceFunction[Any]]] //Class.forName(classSymbol.owner.fullName + s".$name", true, mk.mirror.classLoader).asInstanceOf[Class[SourceFunction[Any]]]
//    val instance = clazz.newInstance()
//
//    val properties = new DataflowNodeProperties(
//      UUID.randomUUID(),
//      DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
//      1,
//      Array[Array[Int]](outputTypeInformation.buildFieldSelectorChain("_1")),
//      Partitioner.PartitioningStrategy.HASH_PARTITIONER,
//      4,
//      name.toString,
//      null,
//      null,
//      outputTypeInformation,
//      clazz,
//      null,
//      null,
//      null,
//      null,
//      null
//    )
//
//    properties
//  }
//
//  def buildSink(f: Map[String, Expr[Any]], p: Expr[Any], qualifier: Qualifier): DataflowNodeProperties = {
//    val inputParam = toParam(f.head._2)
//    val inputType = f.head._2.staticType.typeSymbol
//    val outputType = p.staticType.typeSymbol
//
//    val trans = new Transformer {
//      override def transform(tree: Tree): Tree = tree match {
//        case ident@Ident(name: TermName) => Ident(TermName(name.toString))
//        case _ => super.transform(tree)
//      }
//    }
//
//    val body = trans.transform(p.tree)
//
//    val name = newName("Sink")
//
//    val inputTypeInformation = typeInformation(f.head._2.staticType.asInstanceOf[TypeRef])
//    val outputTypeInformation = typeInformation(p.staticType.asInstanceOf[TypeRef])
//
//    val classTree = q"""
//      class $name extends de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction[Any] {
//        override def consume(in: Any): Unit = {
//          println(in)
//        }
//      }
//    """
//
//    val classSymbol = mk.define(classTree.asInstanceOf[ClassDef])
//    val clazz = Class.forName(classSymbol.owner.fullName + s".$name", true, mk.mirror.classLoader).asInstanceOf[Class[SinkFunction[Any]]]
//    val instance = clazz.newInstance()
//
//    val properties = new DataflowNodeProperties(
//      UUID.randomUUID(),
//      DataflowNodeProperties.DataflowNodeType.UDF_SINK,
//      1,
//      null,
//      null,
//      1,
//      name.toString,
//      inputTypeInformation,
//      null,
//      null,
//      clazz,
//      null,
//      null,
//      null,
//      null,
//      null
//    )
//
//    properties
//  }
//
//  def buildMap(f: Map[String, Expr[Any]], p: Expr[Any], qualifier: Qualifier): DataflowNodeProperties = {
//    val inputParam = toParam(f.head._2)
//    val inputType = f.head._2.staticType.typeSymbol
//    val outputType = p.staticType.typeSymbol
//
//    val trans = new Transformer {
//      override def transform(tree: Tree): Tree = tree match {
//        case ident@Ident(name: TermName) => Ident(TermName(name.toString))
//        case _ => super.transform(tree)
//      }
//    }
//
//    val body = trans.transform(p.tree)
//
//    val name = newName("Mapper")
//
//    val inputTypeInformation = typeInformation(f.head._2.staticType.asInstanceOf[TypeRef])
//    val outputTypeInformation = typeInformation(p.staticType.asInstanceOf[TypeRef])
//
//    val classTree = q"""
//      class $name extends de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction[Any, Any] {
//        override def map(x: Any): Any = {
//          ..$body
//        }
//      }
//    """
//
//    val classSymbol = mk.define(classTree.asInstanceOf[ClassDef])
//    val clazz = Class.forName(classSymbol.owner.fullName + s".$name", true, mk.mirror.classLoader).asInstanceOf[Class[MapFunction[Any, Any]]]
//    val instance = clazz.newInstance()
//
//    //    val instance = create(classTree, inputTypeInformation.`type`, outputTypeInformation.`type`)
//
//    //    println(instance.map("String"))
//
//    val properties = new DataflowNodeProperties(
//      UUID.randomUUID(),
//      DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
//      1,
//      Array[Array[Int]](outputTypeInformation.buildFieldSelectorChain("_1")),
//      Partitioner.PartitioningStrategy.HASH_PARTITIONER,
//      1,
//      name.toString,
//      inputTypeInformation,
//      null,
//      outputTypeInformation,
//      clazz,
//      null,
//      null,
//      null,
//      null,
//      null
//    )
//
//    properties
//  }
//
//  //  def create[I: TypeTag, O: TypeTag](tree: Tree, inTpe: I, outTpe: O): MapFunction[I, O] = {
//  //    val classSymbol = mk.define(tree.asInstanceOf[ClassDef])
//  //    val clazz = Class.forName(classSymbol.owner.fullName + s".U", true, mk.mirror.classLoader).asInstanceOf[Class[MapFunction[I, O]]]
//  //    val instance = clazz.newInstance()
//  //    instance
//  //  }
//
//  def buildFilter(f: Map[String, Expr[Any]], p: Expr[Any], qs: Generator): DataflowNodeProperties = {
//    val valDef = toParam(f.head._2)
//
//    val embedded = q"""
//      final class ${newName("Filter")} extends FilterFunction[${valDef.tpe}] {
//        final def filter($valDef): Boolean = {
//          ..$p
//        }
//      }
//    """
//    //embedded
//    null
//  }
//
//  /**
//   * Introduce support for generic classes and tuple types
//   *
//   *
//   *
//   * @param ref
//   * @return
//   */
//  def typeInformation(ref: Type): TypeInformation = {
//    // TODO: BROKEN!
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
//            new TypeInformation(new de.tuberlin.aura.core.record.tuples.Tuple2[Int, Int]().getClass, JavaConversions.bufferAsJavaList(ListBuffer(children: _*)))
//          //            new TypeInformation(cm.runtimeClass(sym.asClass))
//          case _ => throw new UnsupportedOperationException("")
//        }
//      case a => throw new UnsupportedOperationException("Only TypeRefs supported currently.")
//    }
//  }
//
//  def saveToFile(path: String, code: Tree) = {
//    val writer = new java.io.PrintWriter(path)
//    try writer.write(showCode(code))
//    finally writer.close()
//  }
//
//  def embbedExecutable(clazzes: List[Tree]) = {
//    var i = 0
//
//    val mapping = {
//      val b = Map.newBuilder[TermName, Tree]
//      for (clazz <- clazzes) {
//        i = i + 1
//        b += TermName(s"func_$i") -> clazz
//      }
//      b.result()
//    }
//
//    q"""
//        object Test {
//
//          ..${for (clazz <- clazzes) yield clazz}
//
//          def main(args: Array[String]) {
//            ..${for ((ident, q"$mods class $tpname (...$paramss) extends ..$earlydefns { ..$stats }") <- mapping) yield q"val $ident = new $tpname"}
//          }
//
//        }
//    """
//  }
//}
