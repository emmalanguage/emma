package eu.stratosphere.emma.codegen.flink

import java.net.URI

import eu.stratosphere.emma.api.{CSVInputFormat, CSVOutputFormat}
import eu.stratosphere.emma.codegen.flink.typeutil._
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
          val __input = env.fromCollection(scala.collection.JavaConversions.asJavaCollection(for (v <- values) yield ${tc.srcToTgt(Ident(TermName("v")))}))

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
          for (v <- scala.collection.JavaConversions.collectionAsScalaIterable(collection)) yield ${tc.tgtToSrc(Ident(TermName("v")))}
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
    case op: ir.FlatMap[_, _] => opCode(op)
    case _ => throw new RuntimeException(s"Unsupported ir node of type '${cur.getClass}'")
  }

  private def opCode[OT](op: ir.Read[OT])(implicit closure: DataflowClosure): Tree = {
    val tc = createTypeConvertor(typeOf(op.tag))

    val inFormatTree = op.format match {
      case ifmt: CSVInputFormat[_] =>
        val ptc = createTypeConvertor(typeOf(op.tag)) match {
          case x: ProductTypeConvertor => x
          case _ => throw new RuntimeException(s"Cannot create Flink CsvInputFormat for non-product type ${typeOf(op.tag)}")
        }

        q"""
        {
          // create CsvInputFormat
          val inFormat = new org.apache.flink.api.java.io.CsvInputFormat[${ptc.tgtType}](new org.apache.flink.core.fs.Path(${op.location}))
          inFormat.setDelimiter("\n")
          inFormat.setFieldDelimiter(${ifmt.separator})
          inFormat.setFields(
            Array(..${for (i <- Range(0, ptc.fields.size)) yield Literal(Constant(i))}),
            Array[Class[_]](..${for (t <- ptc.fields) yield t.javaClass}))
          // return to the enclosing code fragment
          inFormat
        }
        """
      case _ =>
        throw new RuntimeException(s"Unsupported InputFormat of type '${op.format.getClass}'")
    }

    // assemble dataflow fragment
    q"""
    {
      // create type information
      val typeInformation = ${tc.tgtTypeInfo}

      // create input format
      val inFormat = $inFormatTree

      // create input
      env.createInput(inFormat, typeInformation)
    }
    """
  }

  private def opCode[IT](op: ir.Write[IT])(implicit closure: DataflowClosure): Tree = {
    val outFormatTree = op.format match {
      case ofmt: CSVOutputFormat[_] =>
        val tc = createTypeConvertor(typeOf(op.xs.tag)) match {
          case x: ProductTypeConvertor => x
          case _ => throw new RuntimeException(s"Cannot create Flink CsvOutputFormat for non-product type ${typeOf(op.xs.tag)}")
        }

        q"""
        new org.apache.flink.api.java.io.CsvOutputFormat[${tc.tgtType}](new org.apache.flink.core.fs.Path(${op.location}), ${ofmt.separator}.toString)
        """
      case _ =>
        throw new RuntimeException(s"Unsupported InputFormat of type '${op.format.getClass}'")
    }

    // assemble dataflow fragment
    q"""
    {
      val __input = ${generateOpCode(op.xs)}

      // create output format
      val outFormat = $outFormatTree

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
    env.fromCollection(scala.collection.JavaConversions.asJavaCollection(for (v <- $inputParam) yield ${tc.srcToTgt(Ident(TermName("v")))}))
    """
  }

  private def opCode[OT, IT](op: ir.Map[OT, IT])(implicit closure: DataflowClosure): Tree = {
    // generate fn UDF
    val fnUDF = new TupleizedUDF(ir.UDF(op.f, tb)) with ReturnedResult
    val fnName = closure.nextUDFName("Mapper")
    closure.closureParams ++= (for (p <- fnUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $fnName(..${fnUDF.closure}) extends org.apache.flink.api.common.functions.RichMapFunction[${fnUDF.paramType(0)}, ${fnUDF.resultType}] with ResultTypeQueryable[${fnUDF.resultType}] {
        override def map(..${fnUDF.params}): ${fnUDF.resultType} = ${fnUDF.body}
        override def getProducedType = ${fnUDF.resultTypeInfo}
      }
      """

    // assemble dataflow fragment
    q"""
    ${generateOpCode(op.xs)}.map(new $fnName(..${fnUDF.closure.map(_.name)}))
    """
  }

  private def opCode[OT, IT](op: ir.FlatMap[OT, IT])(implicit closure: DataflowClosure): Tree = {
    // generate fn UDF
    val fnUDF = new TupleizedUDF(ir.UDF(op.f, tb)) with CollectedResult
    val fnName = closure.nextUDFName("FlatMapper")
    closure.closureParams ++= (for (p <- fnUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $fnName(..${fnUDF.closure}) extends org.apache.flink.api.common.functions.RichFlatMapFunction[${fnUDF.paramType(0)}, ${fnUDF.resultType}] with ResultTypeQueryable[${fnUDF.resultType}] {
        override def flatMap(..${fnUDF.params}, __c: org.apache.flink.util.Collector[${fnUDF.resultType}]): Unit = ${fnUDF.body}
        override def getProducedType = ${fnUDF.resultTypeInfo}
      }
      """

    // assemble dataflow fragment
    q"""
    ${generateOpCode(op.xs)}.flatMap(new $fnName(..${fnUDF.closure.map(_.name)}))
    """
  }

  // --------------------------------------------------------------------------
  // Auxiliary structures
  // --------------------------------------------------------------------------

  private final class DataflowClosure(udfCounter: Counter = new Counter(), scatterCounter: Counter = new Counter()) {

    val closureParams = mutable.Map.empty[TermName, Tree]

    val localInputParams = mutable.Map.empty[TermName, Tree]

    val udfs = mutable.ArrayBuilder.make[Tree]()

    def nextUDFName(prefix: String = "UDF") = TypeName(f"Emma$prefix${udfCounter.advance.get}%05d")

    def nextLocalInputName(prefix: String = "scatter") = TermName(f"scatter${scatterCounter.advance.get}%05d")
  }

  private sealed abstract class TupleizedUDF(val udf: ir.UDF) {

    lazy val paramsTypeConverters = (for (p <- udf.params) yield createTypeConvertor(p.tpt.tpe)).toArray

    lazy val params = for ((p, ptc) <- udf.params zip paramsTypeConverters) yield ValDef(p.mods, p.name, ptc.tgtType, p.rhs)

    def paramType(i: Int) = paramsTypeConverters(i).tgtType

    val resultTypeConverter: TypeConvertor

    lazy val resultType = resultTypeConverter.tgtType

    lazy val resultTypeInfo = resultTypeConverter.tgtTypeInfo

    def closure = udf.closure

    val body: Tree
  }

  private trait ReturnedResult {
    this: TupleizedUDF =>

    val resultTypeConverter = createTypeConvertor(udf.body.tpe)

    val body = {
      var tmp = udf.body
      for ((p, ptc) <- udf.params zip paramsTypeConverters) tmp = ptc.convertTermType(p.name, tmp)
      resultTypeConverter.convertResultType(tmp)
    }
  }

  private trait CollectedResult {
    this: TupleizedUDF =>

    val resultTypeConverter = udf.body.tpe match {
      case TypeRef(prefix, sym, List(tpe)) if bagSymbol == sym =>
        createTypeConvertor(tpe)
      case _ =>
        throw new RuntimeException(s"Unexpected result type ${udf.body.tpe} of UDF with collected result (should return a DataBag).")
    }

    val body = {
      var tmp = udf.body
      // convert the parameter types
      for ((p, ptc) <- udf.params zip paramsTypeConverters) tmp = ptc.convertTermType(p.name, tmp)
      // rewrite the return expression to use the collector instead
      tmp match {
        // complex body consisting of a block
        case Block(stats, Apply(fn, List(seq))) if bagConstructors.contains(fn.symbol) =>
          Block(stats, q"for (x <- $seq) __collect(x)")
        // simple body consisting of a single expression
        case Apply(fn, List(seq)) if bagConstructors.contains(fn.symbol) =>
          q"for (v <- $seq) __c.collect(${resultTypeConverter.srcToTgt(Ident(TermName("v")))})"
        // something unexpected...
        case _ =>
          throw new RuntimeException(s"Unexpected return statement of UDF with collected result (should be a DataBag constructor).")
      }
    }
  }

  private val bagSymbol = rootMirror.staticClass("eu.stratosphere.emma.api.DataBag")

  private val bagConstructors = bagSymbol.companion.info.decl(TermName("apply")).alternatives
}
