package eu.stratosphere.emma.runtime.operators

import java.util.UUID

import de.tuberlin.aura.core.dataflow.api.DataflowAPI.DataflowNodeDescriptor
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties
import de.tuberlin.aura.core.dataflow.udfs.functions.{MapFunction, SinkFunction, SourceFunction}
import de.tuberlin.aura.core.record.Partitioner
import eu.stratosphere.emma.runtime.codegen.utils.TypeConversion._

package object aura {

  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{currentMirror => cm}
  import scala.tools.reflect.ToolBox

  object Generate {

    import scala.tools.reflect.FrontEnd

    val generatedClassDir = "/home/akunft/generated"

    val tb = cm.mkToolBox(
      new FrontEnd {
        def display(info: Info) {}

        def interactive() {}
      },
      s"-d $generatedClassDir"
    )

    def define[A <: Class[_]](name: String, tree: Tree) = {
      val classSymbol = tb.define(tree.asInstanceOf[ClassDef])

      //      val url = Array[URL](new URL("file:///home/akunft/generated/"))
      //      val loader = URLClassLoader.newInstance(url)
      //      val clazz = loader.loadClass(classSymbol.owner.fullName + s".$name").asInstanceOf[A]
      //
      //      val clazz = Class.forName(classSymbol.owner.fullName + s".$name", true, tb.mirror.classLoader)
      //      clazz.newInstance()
      classSymbol.owner.fullName + s".$name"
    }
  }

  trait AuraOperator {
    def generateDescriptors: DataflowNodeDescriptor
  }

  case class Source[A](src: SourceOp[A]) extends AuraOperator {
    val tree = q"""
      class ${src.operatorName} extends de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction[${src.out}] {
        var counter: Int = 1000

        override def produce(): ${src.out} = {
          // hack to produce something
          counter = counter - 1
          if (counter > 0) new de.tuberlin.aura.core.record.tuples.Tuple2(counter, "X") else null
        }
      }
    """

    val outType = typeInformation(src.out)

    val clazz = Generate.define[Class[SourceFunction[A]]](src.operatorName.toString, tree)

    val properties = new DataflowNodeProperties(
      UUID.randomUUID(),
      DataflowNodeProperties.DataflowNodeType.UDF_SOURCE,
      src.operatorName.toString,
      1,
      1,
      Array[Array[Int]](outType.buildFieldSelectorChain("_1")),
      Partitioner.PartitioningStrategy.HASH_PARTITIONER,
      null,
      null,
      outType,
      clazz,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    )

    def generateDescriptors: DataflowNodeDescriptor = {
      new DataflowNodeDescriptor(
        properties
      )
    }
  }

  case class TempSource[A](src: TempSourceOp[A]) extends AuraOperator {
    val dataType = typeInformation(src.out)

    println("SOUCRE " + src.ref.uuid)

    val properties = new DataflowNodeProperties(
      src.ref.uuid,
      DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
      s"DataSet_${src.ref.uuid}",
      1,
      1,
      Array[Array[Int]](dataType.buildFieldSelectorChain("_1")),
      Partitioner.PartitioningStrategy.HASH_PARTITIONER,
      dataType,
      null,
      dataType,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    )

    def generateDescriptors = {
      new DataflowNodeDescriptor(
        properties
      )
    }
  }


  case class Sink[A](snk: SinkOp[A], pre: AuraOperator) extends AuraOperator {
    val tree = q"""
      class ${snk.operatorName} extends de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction[${snk.in}] {
        override def consume(x: ${snk.in}): Unit = {
          println(x)
        }
      }
    """

    val inType = typeInformation(snk.in)

    val properties = new DataflowNodeProperties(
      UUID.randomUUID(),
      DataflowNodeProperties.DataflowNodeType.UDF_SINK,
      snk.operatorName.toString,
      1,
      1,
      Array[Array[Int]](inType.buildFieldSelectorChain("_1")),
      Partitioner.PartitioningStrategy.HASH_PARTITIONER,
      inType,
      null,
      null,
      Generate.define[Class[SinkFunction[A]]](snk.operatorName.toString, tree),
      null,
      null,
      null,
      null,
      null,
      null,
      null
    )

    def generateDescriptors = {
      new DataflowNodeDescriptor(
        properties,
        pre.generateDescriptors
      )
    }
  }

  case class TempSink[A](snk: TempSinkOp[A], pre: AuraOperator) extends AuraOperator {
    val dataType = typeInformation(snk.in)

    println("SINK " + snk.id)

    val properties = new DataflowNodeProperties(
      snk.id,
      DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET,
      s"DataSet_${snk.id}",
      1,
      1,
      Array[Array[Int]](dataType.buildFieldSelectorChain("_1")),
      Partitioner.PartitioningStrategy.HASH_PARTITIONER,
      dataType,
      null,
      dataType,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    )

    override def generateDescriptors: DataflowNodeDescriptor = {
      new DataflowNodeDescriptor(
        properties,
        pre.generateDescriptors
      )
    }
  }


  case class Map[A, B](map: MapOp[A, B], pre: AuraOperator) extends AuraOperator {
    val tree =
      q"""
      final class ${map.operatorName} extends de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction[${map.in}, ${map.out}] {
        override def map(..${map.args}) = {
          ${map.body}
        }
      }
      """

    val inType = typeInformation(map.in)
    val outType = typeInformation(map.out)

    val properties = new DataflowNodeProperties(
      UUID.randomUUID(),
      DataflowNodeProperties.DataflowNodeType.MAP_TUPLE_OPERATOR,
      map.operatorName.toString,
      1,
      1,
      Array[Array[Int]](outType.buildFieldSelectorChain("_1")),
      Partitioner.PartitioningStrategy.HASH_PARTITIONER,
      inType,
      null,
      outType,
      Generate.define[Class[MapFunction[A, B]]](map.operatorName.toString, tree),
      null,
      null,
      null,
      null,
      null,
      null,
      null
    )

    override def generateDescriptors: DataflowNodeDescriptor = {
      new DataflowNodeDescriptor(
        properties,
        pre.generateDescriptors
      )
    }
  }

}
