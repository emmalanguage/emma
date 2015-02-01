package eu.stratosphere.emma.codegen.flink

import java.net.URI

import typeutil._
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir
import eu.stratosphere.emma.util.Counter

import scala.collection.mutable
import scala.reflect.runtime.universe._

class DataflowGenerator(val dataflowCompiler: DataflowCompiler) {

  import eu.stratosphere.emma.runtime.logger

  // get the path where the toolbox will place temp results
  private val tempResultsPrefix = new URI(System.getProperty("emma.temp.dir", s"file://${System.getProperty("java.io.tmpdir")}/emma/temp")).toString

  val tb = dataflowCompiler.tb

  // memoization table
  val memo = mutable.Map[String, ModuleSymbol]()

  // --------------------------------------------------------------------------
  // Scatter & Gather Dataflows (hardcoded)
  // --------------------------------------------------------------------------

  def generateScatterDef[A: TypeTag](id: String) = {
    val dataflowName = s"scatter$$$id"

    memo.getOrElse(dataflowName, {
      logger.info(s"Generating scatter code for '$dataflowName'")

      val tc = createTypeConvertor(typeOf[A].dealias)

      // assemble dataflow tree
      val tree = q"""
      object ${TermName(dataflowName)} {

        import org.apache.flink.api.java.ExecutionEnvironment

        def run(env: ExecutionEnvironment, values: Seq[${tc.srcTpe}]) = {
          val __input = env.fromCollection(scala.collection.JavaConversions.asJavaCollection(for (v <- values) yield ${tc.srcToTgt(TermName("v"))}))

          // create type information
          val typeInformation = ${tc.tgtTypeInfo}

          // create output format
          val outFormat = new org.apache.flink.api.java.io.TypeSerializerOutputFormat[${tc.tgtType}]
          outFormat.setInputType(typeInformation)
          outFormat.setSerializer(typeInformation.createSerializer())

          // write output
          __input.write(outFormat, ${s"$tempResultsPrefix/$id"}, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)

          env.execute(${s"Emma[$dataflowName]"})
        }
      }
      """.asInstanceOf[ImplDef]

      // compile the dataflow tree
      val symbol = dataflowCompiler.compile(tree)
      // update the memo table
      memo.put(dataflowName, symbol)
      // return the compiled dataflow symbol
      symbol
    })
  }

  def generateGatherDef[A: TypeTag](id: String) = {
    val dataflowName = s"gather$$$id"

    memo.getOrElse(dataflowName, {
      logger.info(s"Generating gather code for '$dataflowName'")

      val tc = createTypeConvertor(typeOf[A].dealias)

      // assemble dataflow
      val tree = q"""
      object ${TermName(s"gather$$$dataflowName")} {

        import org.apache.flink.api.java.ExecutionEnvironment
        import scala.collection.JavaConverters._

        def run(env: ExecutionEnvironment) = {
          // create type information
          val typeInformation = ${tc.tgtTypeInfo}

          // create input format
          val inFormat = new org.apache.flink.api.java.io.TypeSerializerInputFormat[${tc.tgtType}](typeInformation.createSerializer())
          inFormat.setFilePath(${s"$tempResultsPrefix/$id"})

          // create input
          val in = env.createInput(inFormat, typeInformation)

          // local collection to store results in
          val collection = java.util.Collections.synchronizedCollection(new java.util.ArrayList[${tc.tgtType}]())

          // collect results from remote in local collection
          org.apache.flink.api.java.io.RemoteCollectorImpl.collectLocal(in, collection)

          // execute gather dataflow
          env.execute(${s"Emma[$dataflowName]"})

          // construct result DataBag
          for (v <- scala.collection.JavaConversions.collectionAsScalaIterable(collection)) yield ${tc.tgtToSrc(TermName("v"))}
        }
      }
      """.asInstanceOf[ImplDef]

      // compile the dataflow tree
      val symbol = dataflowCompiler.compile(tree)
      // update the memo table
      memo.put(dataflowName, symbol)
      // return the compiled dataflow symbol
      symbol
    })
  }

  // --------------------------------------------------------------------------
  // Combinator Dataflows (traversal based)
  // --------------------------------------------------------------------------

  def generateDataflowDef(root: ir.Combinator[_], id: String) = {
    val dataflowName = id

    memo.getOrElse(dataflowName, {
      logger.info(s"Generating dataflow code for '$dataflowName'")

      // initialize UDF store to be passed around implicitly during dataflow opCode assembly
      implicit val closure = new DataflowClosure()
      // generate dataflow operator assembly code
      val opCode = generateOpCode(root)
      // create a sorted list of closure parameters (convention used by client in the generated macro)
      val params = for (p <- closure.closureParams.toSeq.sortBy(_._1.toString)) yield ValDef(Modifiers(Flag.PARAM), p._1, p._2, EmptyTree)
      // create a sorted list of closure local inputs (convention used by the Engine client)
      val localInputs = for (p <- closure.localInputParams.toSeq.sortBy(_._1.toString)) yield ValDef(Modifiers(Flag.PARAM), p._1, p._2, EmptyTree)

      // assemble dataflow
      val tree = q"""
      object ${TermName(dataflowName)} {

        import org.apache.flink.api.java.ExecutionEnvironment
        import org.apache.flink.api.java.typeutils.ResultTypeQueryable

        ..${closure.udfs.result().toSeq}

        def run(env: ExecutionEnvironment, ..$params, ..$localInputs) = {
          $opCode

          env.execute("Emma[" + $dataflowName + "]")
        }
      }
      """.asInstanceOf[ImplDef]

      // compile the dataflow tree
      val symbol = dataflowCompiler.compile(tree)
      // update the memo table
      memo.put(dataflowName, symbol)
      // return the compiled dataflow symbol
      symbol
    })
  }

  private def generateOpCode(cur: ir.Combinator[_])(implicit closure: DataflowClosure): Tree = cur match {
    case op: ir.Read[_] => opCode(op)
    case op: ir.Write[_] => opCode(op)
    case op: ir.TempSource[_] => opCode(op)
    case op: ir.TempSink[_] => opCode(op)
    case op: ir.Scatter[_] => opCode(op)
    case op: ir.Map[_, _] => opCode(op)
    case _ => throw new RuntimeException(s"Unsupported ir node of type '${cur.getClass}'")
  }

  private def opCode[OT](op: ir.Read[OT])(implicit closure: DataflowClosure): Tree = {
    val tc = createTypeConvertor(typeOf(op.tag)) match {
      case tc: ProductTypeConvertor => tc
      case _ => throw new RuntimeException(s"Cannot create Flink CsvInputFormat for non-product type ${typeOf(op.tag)}")
    }

    // TODO: the input format assembly should be extensible
    // assemble dataflow fragment
    q"""
    {
      // create type information
      val typeInformation = ${tc.tgtTypeInfo}

      // create input format
      val inFormat = new org.apache.flink.api.java.io.CsvInputFormat[${tc.tgtType}](new org.apache.flink.core.fs.Path(${op.location}))
      inFormat.setDelimiter("\n")
      inFormat.setFieldDelimiter('\t')
      inFormat.setFields(
        Array(..${for (i <- Range(0, tc.fields.size)) yield Literal(Constant(i))}),
        Array[Class[_]](..${for (t <- tc.fields) yield t.javaClass}))

      // create input
      env.createInput(inFormat, typeInformation)
    }
    """
  }

  private def opCode[IT](op: ir.Write[IT])(implicit closure: DataflowClosure): Tree = {
    val tc = createTypeConvertor(typeOf(op.xs.tag))

    // TODO: the output format assembly should be extensible
    // assemble dataflow fragment
    q"""
    {
      val __input = ${generateOpCode(op.xs)}

      // create output format
      val outFormat = new org.apache.flink.api.java.io.CsvOutputFormat[${tc.tgtType}](new org.apache.flink.core.fs.Path(${op.location}))

      // write output
      __input.write(outFormat, ${op.location}, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
    }
    """
  }

  private def opCode[OT](op: ir.TempSource[OT])(implicit closure: DataflowClosure): Tree = {
    val tc = createTypeConvertor(typeOf(op.tag))

    // assemble dataflow fragment
    q"""
    {
      // create type information
      val typeInformation = ${tc.tgtTypeInfo}

      // create input format
      val inFormat = new org.apache.flink.api.java.io.TypeSerializerInputFormat[${tc.tgtType}](typeInformation.createSerializer())
      inFormat.setFilePath(${s"$tempResultsPrefix/${op.ref.name}"})

      // create input
      env.createInput(inFormat, typeInformation)
    }
    """
  }

  private def opCode[IT](op: ir.TempSink[IT])(implicit closure: DataflowClosure): Tree = {
    val tc = createTypeConvertor(typeOf(op.tag))

    // assemble dataflow fragment
    q"""
    {
      val __input = ${generateOpCode(op.xs)}

      // create type information
      val typeInformation = ${tc.tgtTypeInfo}

      // create output format
      val outFormat = new org.apache.flink.api.java.io.TypeSerializerOutputFormat[${tc.tgtType}]
      outFormat.setInputType(typeInformation)
      outFormat.setSerializer(typeInformation.createSerializer())

      // write output
      __input.write(outFormat, ${s"$tempResultsPrefix/${op.name}"}, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
    }
    """
  }

  private def opCode[OT](op: ir.Scatter[OT])(implicit closure: DataflowClosure): Tree = {
    val tc = createTypeConvertor(typeOf(op.tag))

    // add a dedicated closure variable to pass the scattered term
    val inputParam = closure.nextLocalInputName()
    closure.localInputParams += inputParam -> tq"Seq[${tc.srcTpe}]"

    // assemble dataflow fragment
    q"""
    env.fromCollection(scala.collection.JavaConversions.asJavaCollection(for (v <- $inputParam) yield ${tc.srcToTgt(TermName("v"))}))
    """
  }

  private def opCode[OT, IT](op: ir.Map[OT, IT])(implicit closure: DataflowClosure): Tree = {
    // generate mapper UDF
    val mapperUDF = TupleizedUDF(ir.UDF(op.f, tb))
    val mapperName = closure.nextUDFName("Mapper")
    closure.closureParams ++= (for (p <- mapperUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $mapperName(..${mapperUDF.closure}) extends org.apache.flink.api.common.functions.RichMapFunction[${mapperUDF.paramType(0)}, ${mapperUDF.resultType}] with ResultTypeQueryable[${mapperUDF.resultType}] {
        override def map(..${mapperUDF.params}): ${mapperUDF.resultType} = ${mapperUDF.body}
        override def getProducedType = ${mapperUDF.resultTypeInfo}
      }
      """

    // assemble dataflow fragment
    q"""
    ${generateOpCode(op.xs)}.map(new $mapperName(..${mapperUDF.closure.map(_.name)}))
    """
  }

  // --------------------------------------------------------------------------
  // Auxiliary structures
  // --------------------------------------------------------------------------

  final class DataflowClosure(udfCounter: Counter = new Counter(), scatterCounter: Counter = new Counter()) {

    val closureParams = mutable.Map.empty[TermName, Tree]

    val localInputParams = mutable.Map.empty[TermName, Tree]

    val udfs = mutable.ArrayBuilder.make[Tree]()

    def nextUDFName(prefix: String = "UDF") = TypeName(f"Emma$prefix${udfCounter.advance.get}%05d")

    def nextLocalInputName(prefix: String = "scatter") = TermName(f"scatter${scatterCounter.advance.get}%05d")
  }

  final class TupleizedUDF(udf: ir.UDF) {

    val paramsTypeConverters = (for (p <- udf.params) yield createTypeConvertor(p.tpt.tpe)).toArray

    val resultTypeConverter = createTypeConvertor(udf.body.tpe)

    val resultType = resultTypeConverter.tgtType

    val resultTypeInfo = resultTypeConverter.tgtTypeInfo

    val params = for ((p, ptc) <- udf.params zip paramsTypeConverters) yield ValDef(p.mods, p.name, ptc.tgtType, p.rhs)

    def paramType(i: Int) = paramsTypeConverters(i).tgtType

    def closure = udf.closure

    val body = {
      var tmp = udf.body
      for ((p, ptc) <- udf.params zip paramsTypeConverters) tmp = ptc.convertTermType(p.name, tmp)
      resultTypeConverter.convertResultType(tmp)
    }
  }

  object TupleizedUDF {

    def apply(udf: ir.UDF) = new TupleizedUDF(udf)
  }

}
