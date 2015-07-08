package eu.stratosphere.emma.codegen.flink

import java.net.URI
import java.util.UUID

import eu.stratosphere.emma.api.{CSVInputFormat, CSVOutputFormat}
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir
import eu.stratosphere.emma.util.Counter
import org.apache.commons.io.FilenameUtils

import scala.collection.mutable
import scala.reflect.runtime.universe._

class DataflowGenerator(val dataflowCompiler: DataflowCompiler, val sessionID: UUID = UUID.randomUUID()) {

  import eu.stratosphere.emma.runtime.logger

  // get the path where the toolbox will place temp results
  private val tempResultsPrefix = new URI(System.getProperty("emma.temp.dir", s"file:///${FilenameUtils.separatorsToUnix(System.getProperty("java.io.tmpdir"))}/emma/temp")).toString

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

      val tpe = typeOf[A]

      // assemble dataflow tree
      val tree = q"""
      object ${TermName(dataflowName)} {

        import _root_.org.apache.flink.api.scala.ExecutionEnvironment
        import _root_.org.apache.flink.api.scala._

        def run(env: ExecutionEnvironment, values: Seq[$tpe]) = {
          // read the input
          env.fromCollection(values)
        }
      }
      """.asInstanceOf[ImplDef]

      // compile the dataflow tree
      val symbol = dataflowCompiler.compile(tree).asModule
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

      val tpe = typeOf[A]

      // assemble dataflow
      val tree = q"""
      object ${TermName(dataflowName)} {

        import org.apache.flink.api.scala.ExecutionEnvironment
        import org.apache.flink.api.scala._

        def run(env: ExecutionEnvironment, expr: DataSet[$tpe]) = {
          eu.stratosphere.emma.api.DataBag[$tpe](expr.collect)
        }
      }
      """.asInstanceOf[ImplDef]

      // compile the dataflow tree
      val symbol = dataflowCompiler.compile(tree).asModule
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

      val runMethod = root match {
        case op: ir.Fold[_, _] =>
          q"""
          def run(env: ExecutionEnvironment, ..$params, ..$localInputs) = {
            $opCode
          }
          """
        case op: ir.TempSink[_] =>
          q"""
          def run(env: ExecutionEnvironment, ..$params, ..$localInputs) = {
            val __result = $opCode
            env.execute(${s"Emma[$sessionID][$dataflowName]"})
            __result
          }
          """
        case op: ir.Write[_] =>
          q"""
          def run(env: ExecutionEnvironment, ..$params, ..$localInputs): Unit = {
            val __result = $opCode
            env.execute(${s"Emma[$sessionID][$dataflowName]"})
          }
          """
        case _ =>
          throw new RuntimeException(s"Unsupported root IR node type '${root.getClass.getCanonicalName}'")
      }

      // assemble dataflow
      val tree = q"""
      object ${TermName(dataflowName)} {

        import org.apache.flink.api.scala.ExecutionEnvironment
        import org.apache.flink.api.scala._
        import org.apache.flink.api.java.typeutils.ResultTypeQueryable
        import eu.stratosphere.emma.api.DataBag

        ..${closure.udfs.result().toSeq}

        $runMethod

        def clean[F](env: ExecutionEnvironment, f: F): F = {
          if (env.getConfig.isClosureCleanerEnabled) {
            org.apache.flink.api.java.ClosureCleaner.clean(f, true)
          }
          org.apache.flink.api.java.ClosureCleaner.ensureSerializable(f)
          f
        }
      }
      """.asInstanceOf[ImplDef]

      // compile the dataflow tree
      val symbol = dataflowCompiler.compile(tree).asModule
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
    case op: ir.Filter[_] => opCode(op)
    case op: ir.EquiJoin[_, _, _] => opCode(op)
    case op: ir.Cross[_, _, _] => opCode(op)
    case op: ir.Fold[_, _] => opCode(op)
    case op: ir.FoldGroup[_, _] => opCode(op)
    case op: ir.Distinct[_] => opCode(op)
    case op: ir.Union[_] => opCode(op)
    case op: ir.Group[_, _] => opCode(op)
    case _ => throw new RuntimeException(s"Unsupported ir node of type '${cur.getClass}'")
  }

  private def opCode[OT](op: ir.Read[OT])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias

    val inFormatTree = op.format match {
      case ifmt: CSVInputFormat[_] =>
        if (!(tpe <:< weakTypeOf[Product])) {
          throw new RuntimeException(s"Cannot create Flink CsvInputFormat for non-product type ${typeOf(op.tag)}")
        }

        q"""
        {
          val inFormat = new org.apache.flink.api.scala.operators.ScalaCsvInputFormat[$tpe](new org.apache.flink.core.fs.Path(${op.location}), typeInformation)
          inFormat.setFieldDelimiter(${ifmt.separator})

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
      val typeInformation = createTypeInformation[$tpe]

      // create input format
      val inFormat = $inFormatTree

      // create input
      env.createInput(inFormat)
    }
    """
  }

  private def opCode[IT](op: ir.Write[IT])(implicit closure: DataflowClosure): Tree = {
    val tpe = typeOf(op.xs.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    val outFormatTree = op.format match {
      case ofmt: CSVOutputFormat[_] =>
        if (!(tpe <:< weakTypeOf[Product])) {
          throw new RuntimeException(s"Cannot create Flink CsvOutputFormat for non-product type $tpe")
        }

        q"""
        new org.apache.flink.api.scala.operators.ScalaCsvOutputFormat[$tpe](new org.apache.flink.core.fs.Path(${op.location}), ${ofmt.separator}.toString)
        """
      case _ =>
        throw new RuntimeException(s"Unsupported InputFormat of type '${op.format.getClass}'")
    }

    // assemble dataflow fragment
    q"""
    {
      val __input = $xs

      // create output format
      val outFormat = $outFormatTree

      // write output
      __input.write(outFormat, ${op.location}, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
    }
    """
  }

  private def opCode[OT](op: ir.TempSource[OT])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias

    // add a dedicated closure variable to pass the input param
    val param = TermName(op.ref.name)
    closure.closureParams += param -> tq"eu.stratosphere.emma.runtime.Flink.DataBagRef[$tpe]"

    // assemble dataflow fragment
    q"""
    {
      // create type information
      val typeInformation = createTypeInformation[$tpe]

      // create input format
      val inFormat = new org.apache.flink.api.java.io.TypeSerializerInputFormat[$tpe](typeInformation)
      inFormat.setFilePath(${s"$tempResultsPrefix/${op.ref.name}"})

      // create input
      env.createInput(inFormat)
    }
    """
  }

  private def opCode[IT](op: ir.TempSink[IT])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // assemble dataflow fragment
    q"""
    {
      val __input = $xs

      // create type information
      val typeInformation = createTypeInformation[$tpe]

      // create output format
      val outFormat = new org.apache.flink.api.java.io.TypeSerializerOutputFormat[$tpe]
      outFormat.setInputType(typeInformation, env.getConfig)
      outFormat.setSerializer(typeInformation.createSerializer(env.getConfig))

      // write output
      __input.write(outFormat, ${s"$tempResultsPrefix/${op.name}"}, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)

      // return a reference to the original input
      __input
    }
    """
  }

  private def opCode[OT](op: ir.Scatter[OT])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias

    // add a dedicated closure variable to pass the scattered term
    val inputParam = closure.nextLocalInputName()
    closure.localInputParams += inputParam -> tq"Seq[$tpe]"

    // assemble dataflow fragment
    q"""
    env.fromCollection($inputParam)
    """
  }

  private def opCode[OT, IT](op: ir.Map[OT, IT])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val srcTpe = typeOf(op.xs.tag).dealias
    val tgtTpe = typeOf(op.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // generate fn UDF
    val f = typecheckUDF(op.f)
    val fUDF = ir.UDF(f, f.tpe, tb)
    val fnName = closure.nextUDFName("Mapper")
    closure.closureParams ++= (for (p <- fUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $fnName(..${fUDF.closure}) extends org.apache.flink.api.common.functions.RichMapFunction[$srcTpe, $tgtTpe] with ResultTypeQueryable[$tgtTpe] {
        override def map(..${fUDF.params}): $tgtTpe = ${fUDF.body}
        override def getProducedType = createTypeInformation[$tgtTpe]
      }
      """

    // assemble dataflow fragment
    q"""
    $xs.map(new $fnName(..${fUDF.closure.map(_.name)}))
    """
  }

  private def opCode[OT, IT](op: ir.FlatMap[OT, IT])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val srcTpe = typeOf(op.xs.tag).dealias
    val tgtTpe = typeOf(op.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // generate fn UDF
    val f = typecheckUDF(op.f)
    val fUDF = ir.UDF(f, f.tpe, tb)
    val fnName = closure.nextUDFName("FlatMapper")
    closure.closureParams ++= (for (p <- fUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $fnName(..${fUDF.closure}) extends org.apache.flink.api.common.functions.RichFlatMapFunction[$srcTpe, $tgtTpe] with ResultTypeQueryable[$tgtTpe] {
        override def flatMap(..${fUDF.params}, __c: org.apache.flink.util.Collector[$tgtTpe]): Unit = {
          for (x <- (${fUDF.body}).fetch()) __c.collect(x)
        }
        override def getProducedType = createTypeInformation[$tgtTpe]
      }
      """

    // assemble dataflow fragment
    q"""
    $xs.flatMap(new $fnName(..${fUDF.closure.map(_.name)}))
    """
  }

  private def opCode[T](op: ir.Filter[T])(implicit closure: DataflowClosure): Tree = {
    val tpe = typeOf(op.tag)

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // generate fn UDF
    val p = typecheckUDF(op.p)
    val pUDF = ir.UDF(p, p.tpe, tb)
    val pName = closure.nextUDFName("Filter")
    closure.closureParams ++= (for (p <- pUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $pName(..${pUDF.closure}) extends org.apache.flink.api.common.functions.RichFilterFunction[$tpe] {
        override def filter(..${pUDF.params}): Boolean = ${pUDF.body}
      }
      """

    // assemble dataflow fragment
    q"""
    $xs.filter(new $pName(..${pUDF.closure.map(_.name)}))
    """
  }

  private def opCode[OT, IT1, IT2](op: ir.EquiJoin[OT, IT1, IT2])(implicit closure: DataflowClosure): Tree = {
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)

    val keyx = typecheckUDF(op.keyx)
    val keyy = typecheckUDF(op.keyy)

    // assemble dataflow fragment
    q"""
    val __xs = $xs
    val __ys = $ys

    __xs join __ys where { ${keyx.asInstanceOf[Function].body} } equalTo { ${keyy.asInstanceOf[Function].body} }
    """
  }

  private def opCode[OT, IT1, IT2](op: ir.Cross[OT, IT1, IT2])(implicit closure: DataflowClosure): Tree = {
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)

    // assemble dataflow fragment
    q"""
    val __xs = $xs
    val __ys = $ys

    __xs cross __ys
    """
  }

  private def opCode[OT, IT](op: ir.Fold[OT, IT])(implicit closure: DataflowClosure): Tree = {
    val srcTpe = typeOf(op.xs.tag).dealias
    val tgtTpe = typeOf(op.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // get fold components
    val empty = typecheckUDF(op.empty)
    val sng = typecheckUDF(op.sng)
    val union = typecheckUDF(op.union)

    val mapName = closure.nextUDFName("FoldMapper")
    val mapUDF = ir.UDF(sng, sng.tpe.dealias, tb)
    val mapTpe = tgtTpe
    closure.closureParams ++= (for (p <- mapUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $mapName(..${mapUDF.closure}) extends org.apache.flink.api.common.functions.RichMapFunction[$srcTpe, $mapTpe] with ResultTypeQueryable[$mapTpe] {
        override def map(..${mapUDF.params}): $mapTpe = ${mapUDF.body}
        override def getProducedType = createTypeInformation[$mapTpe]
      }
      """

    val reduceName = closure.nextUDFName("FoldReducer")
    val reduceUDF = ir.UDF(union, union.tpe.dealias, tb)
    val reduceTpe = tgtTpe
    closure.closureParams ++= (for (p <- reduceUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $reduceName(..${reduceUDF.closure}) extends org.apache.flink.api.common.functions.RichReduceFunction[$reduceTpe] with ResultTypeQueryable[$reduceTpe] {
        override def reduce(..${reduceUDF.params}): $reduceTpe = ${reduceUDF.body}
        override def getProducedType = createTypeInformation[$reduceTpe]
      }
      """

    // assemble dataflow fragment
    q"""
    val __result = $xs
      .map(new $mapName(..${mapUDF.closure.map(_.name)}))
      .reduce(new $reduceName(..${reduceUDF.closure.map(_.name)}))
      .collect

    if (__result.isEmpty) ${empty.asInstanceOf[Function].body} else __result.head
    """
  }

  private def opCode[OT, IT](op: ir.FoldGroup[OT, IT])(implicit closure: DataflowClosure): Tree = {
    val srcTpe = typeOf(op.xs.tag).dealias
    val tgtTpe = typeOf(op.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // get fold components
    val key = typecheckUDF(op.key)
    val keyUDF = ir.UDF(key, key.tpe.dealias, tb)
    val sng = typecheckUDF(op.sng)
    val sngUDF = ir.UDF(sng, sng.tpe.dealias, tb)
    val union = typecheckUDF(op.union)
    val unionUDF = ir.UDF(union, union.tpe.dealias, tb)

    val mapName = closure.nextUDFName("FoldGroupMapper")
    val mapTpe = tgtTpe
    closure.udfs +=
      q"""
      class $mapName extends org.apache.flink.api.common.functions.RichMapFunction[$srcTpe, $mapTpe] with ResultTypeQueryable[$mapTpe] {
        override def map(x: $srcTpe): $mapTpe = new $mapTpe(extractKey(x), mapAggregates(x))
        override def getProducedType = createTypeInformation[$mapTpe]
        def extractKey(..${keyUDF.params}) = ${keyUDF.body}
        def mapAggregates(..${sngUDF.params}) = ${sngUDF.body}
      }
      """

    val reduceName = closure.nextUDFName("FoldGroupReducer")
    val reduceUDF = {
      // FIXME: this is not sanitized, input parameters might diverge
      // assemble UDF code
      val udf = tb.typecheck(tb.untypecheck(
        q"""
        () => (x: $tgtTpe, y: $tgtTpe) => new $tgtTpe(
          x.key, ${
          substitute(
            unionUDF.params(0).name -> q"x.values",
            unionUDF.params(1).name -> q"y.values")(unionUDF.body)
        })
        """))
      // construct UDF
      ir.UDF(udf.asInstanceOf[Function], udf.tpe, tb)
    }
    val reduceTpe = tgtTpe
    closure.closureParams ++= (for (p <- reduceUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $reduceName(..${reduceUDF.closure}) extends org.apache.flink.api.common.functions.RichReduceFunction[$reduceTpe] with ResultTypeQueryable[$reduceTpe] {
        override def reduce(..${reduceUDF.params}): $reduceTpe = ${reduceUDF.body}
        override def getProducedType = createTypeInformation[$reduceTpe]
      }
      """

    // assemble dataflow fragment
    q"""
    $xs
      .map(new $mapName())
      .groupBy(_.key)
      .reduce(new $reduceName(..${reduceUDF.closure.map(_.name)}))
    """
  }

  private def opCode[T](op: ir.Distinct[T])(implicit closure: DataflowClosure): Tree = {
    val tgtTpe = typeOf(op.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // assemble dataflow fragment
    q"$xs.distinct((v: $tgtTpe) => v)"
  }

  private def opCode[T](op: ir.Union[T])(implicit closure: DataflowClosure): Tree = {
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)

    // assemble dataflow fragment
    q"$xs union $ys"
  }

  private def opCode[OT, IT](op: ir.Group[OT, IT])(implicit closure: DataflowClosure): Tree = {
    val srcTpe = typeOf(op.xs.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // generate key UDF
    val keyFn  = typecheckUDF(op.key)
    val keyUDF = ir.UDF(keyFn, keyFn.tpe, tb)
    closure.closureParams ++=
      (for (p <- keyUDF.closure) yield p.name -> p.tpt)

    val iterator = freshIdent("iterator")
    val stream   = freshIdent("stream")

    // assemble dataflow fragment
    q"""$xs groupBy { (..${keyUDF.params}) =>
        ${keyUDF.body}
      } reduceGroup { ($iterator: scala.Iterator[$srcTpe]) =>
        val $stream = $iterator.toStream
        eu.stratosphere.emma.api.Group(
          ((..${keyUDF.params}) => ${keyUDF.body})($stream.head),
          eu.stratosphere.emma.api.DataBag($stream))
      }"""
  }

  private def typecheckUDF(udf: String) = tb.typecheck(tb.parse(udf))

  private def freshIdent(prefix: String) =
    Ident(internal.reificationSupport.freshTermName(prefix))

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

  def substitute(map: (TermName, Tree)*) = new SymbolSubstituter(Map(map: _*))

  class SymbolSubstituter(map: Map[TermName, Tree]) extends Transformer with (Tree => Tree) {
    override def apply(tree: Tree): Tree = transform(tree)

    override def transform(tree: Tree) = tree match {
      case Ident(name@TermName(_)) if map.contains(name) => map(name)
      case _ => super.transform(tree)
    }
  }

}
