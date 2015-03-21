package eu.stratosphere.emma.codegen.flink

import java.net.URI
import java.nio.file.Paths
import java.util.UUID

import eu.stratosphere.emma.api.{CSVInputFormat, CSVOutputFormat}
import eu.stratosphere.emma.codegen.flink.typeutil._
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

  val typeInfoFactory = new TypeInformationFactory(dataflowCompiler)

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
      val tpeInfo = typeInfoFactory(tpe)

      // assemble dataflow tree
      val tree = q"""
      object ${TermName(dataflowName)} {

        import org.apache.flink.api.java.ExecutionEnvironment

        def run(env: ExecutionEnvironment, values: Seq[$tpe]) = {
          // create type information
          val typeInformation = $tpeInfo

          // read the input
          val __input = env.fromCollection(scala.collection.JavaConversions.asJavaCollection(values), typeInformation)

          // create output format
          val outFormat = new org.apache.flink.api.java.io.TypeSerializerOutputFormat[$tpe]
          outFormat.setInputType(typeInformation, env.getConfig)
          outFormat.setSerializer(typeInformation.createSerializer(env.getConfig))

          // write output
          __input.write(outFormat, ${s"$tempResultsPrefix/$id"}, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)

          env.execute(${s"Emma[$sessionID][$dataflowName]"})
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
      val tpeInfo = typeInfoFactory(tpe)

      // assemble dataflow
      val tree = q"""
      object ${TermName(dataflowName)} {

        import org.apache.flink.api.java.ExecutionEnvironment
        import scala.collection.JavaConverters._

        def run(env: ExecutionEnvironment) = {
          // create type information
          val typeInformation = $tpeInfo

          // create input format
          val inFormat = new org.apache.flink.api.java.io.TypeSerializerInputFormat[$tpe](typeInformation)
          inFormat.setFilePath(${s"$tempResultsPrefix/$id"})

          // create input
          val in = env.createInput(inFormat, typeInformation)

          // local collection to store results in
          val collection = java.util.Collections.synchronizedCollection(new java.util.ArrayList[$tpe]())

          // collect results from remote in local collection
          org.apache.flink.api.java.io.RemoteCollectorImpl.collectLocal(in, collection)

          // execute gather dataflow
          env.execute(${s"Emma[$sessionID][$dataflowName]"})

          // construct result Seq
          scala.collection.JavaConversions.collectionAsScalaIterable(collection).toStream
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
          val empty = tb.typecheck(tb.parse(op.empty))
          q"""
          def run(env: ExecutionEnvironment, ..$params, ..$localInputs) = {
            val __foldCollection = $opCode
            env.execute(${s"Emma[$sessionID][$dataflowName]"})
            if (__foldCollection.isEmpty) $empty else __foldCollection.iterator().next()
          }
          """
        case op: ir.Combinator[_] =>
          q"""
          def run(env: ExecutionEnvironment, ..$params, ..$localInputs) = {
            $opCode
            env.execute(${s"Emma[$sessionID][$dataflowName]"})
          }
          """
      }

      // assemble dataflow
      val tree = q"""
      object ${TermName(dataflowName)} {

        import org.apache.flink.api.java.ExecutionEnvironment
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
    case _ => throw new RuntimeException(s"Unsupported ir node of type '${cur.getClass}'")
  }

  private def opCode[OT](op: ir.Read[OT])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias
    val tpeInfo = typeInfoFactory(tpe)

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
      val typeInformation = $tpeInfo

      // create input format
      val inFormat = $inFormatTree

      // create input
      env.createInput(inFormat, typeInformation)
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
    val tpeInfo = typeInfoFactory(tpe)

    // add a dedicated closure variable to pass the input param
    val param = TermName(op.ref.name)
    closure.closureParams += param -> tq"eu.stratosphere.emma.runtime.Flink.DataBagRef[$tpe]"

    // assemble dataflow fragment
    q"""
    {
      // create type information
      val typeInformation = $tpeInfo

      // create input format
      val inFormat = new org.apache.flink.api.java.io.TypeSerializerInputFormat[$tpe](typeInformation)
      inFormat.setFilePath(${s"$tempResultsPrefix/${op.ref.name}"})

      // create input
      env.createInput(inFormat, typeInformation)
    }
    """
  }

  private def opCode[IT](op: ir.TempSink[IT])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias
    val tpeInfo = typeInfoFactory(tpe)

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // assemble dataflow fragment
    q"""
    {
      val __input = $xs

      // create type information
      val typeInformation = $tpeInfo

      // create output format
      val outFormat = new org.apache.flink.api.java.io.TypeSerializerOutputFormat[$tpe]
      outFormat.setInputType(typeInformation, env.getConfig)
      outFormat.setSerializer(typeInformation.createSerializer(env.getConfig))

      // write output
      __input.write(outFormat, ${s"$tempResultsPrefix/${op.name}"}, org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
    }
    """
  }

  private def opCode[OT](op: ir.Scatter[OT])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias
    val tpeInfo = typeInfoFactory(tpe)

    // add a dedicated closure variable to pass the scattered term
    val inputParam = closure.nextLocalInputName()
    closure.localInputParams += inputParam -> tq"Seq[$tpe]"

    // assemble dataflow fragment
    q"""
    env.fromCollection(scala.collection.JavaConversions.asJavaCollection($inputParam), $tpeInfo)
    """
  }

  private def opCode[OT, IT](op: ir.Map[OT, IT])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val srcTpe = typeOf(op.xs.tag).dealias
    val tgtTpe = typeOf(op.tag).dealias
    val tgtTpeInfo = typeInfoFactory(tgtTpe)

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // generate fn UDF
    val f = tb.typecheck(tb.parse(op.f))
    val fUDF = ir.UDF(f, f.tpe, tb)
    val fnName = closure.nextUDFName("Mapper")
    closure.closureParams ++= (for (p <- fUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $fnName(..${fUDF.closure}) extends org.apache.flink.api.common.functions.RichMapFunction[$srcTpe, $tgtTpe] with ResultTypeQueryable[$tgtTpe] {
        override def map(..${fUDF.params}): $tgtTpe = ${fUDF.body}
        override def getProducedType = $tgtTpeInfo
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
    val tgtTpeInfo = typeInfoFactory(tgtTpe)

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // generate fn UDF
    val f = tb.typecheck(tb.parse(op.f))
    val fUDF = ir.UDF(f, f.tpe, tb)
    val fnName = closure.nextUDFName("FlatMapper")
    closure.closureParams ++= (for (p <- fUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $fnName(..${fUDF.closure}) extends org.apache.flink.api.common.functions.RichFlatMapFunction[$srcTpe, $tgtTpe] with ResultTypeQueryable[$tgtTpe] {
        override def flatMap(..${fUDF.params}, __c: org.apache.flink.util.Collector[$tgtTpe]): Unit = {
          for (x <- (${fUDF.body}).fetch()) __c.collect(x)
        }
        override def getProducedType = $tgtTpeInfo
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
    val p = tb.typecheck(tb.parse(op.p))
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
    // infer types and generate type information
    val xsTpe = typeOf(op.xs.tag).dealias
    val ysTpe = typeOf(op.ys.tag).dealias

    val keyx = tb.typecheck(tb.parse(op.keyx))
    val keyxUDF = ir.UDF(keyx, keyx.tpe.dealias, tb)
    val keyxTpe = ir.resultType(keyx.tpe.dealias)
    val keyxTpeInfo = typeInfoFactory(keyxTpe)

    val keyy = tb.typecheck(tb.parse(op.keyy))
    val keyyUDF = ir.UDF(keyy, keyy.tpe.dealias, tb)
    val keyyTpe = ir.resultType(keyy.tpe.dealias)
    val keyyTpeInfo = typeInfoFactory(keyyTpe)

    val f = tb.typecheck(tb.parse(op.f))
    val fUDF = ir.UDF(f, f.tpe.dealias, tb)
    val fTpe = ir.resultType(f.tpe.dealias)
    val fTpeInfo = typeInfoFactory(typeOf(op.tag).dealias)

    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)

    // generate keyx UDF
    val keyxName = closure.nextUDFName("KeyX")
    closure.closureParams ++= (for (p <- keyxUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $keyxName(..${keyxUDF.closure}) extends org.apache.flink.api.java.functions.KeySelector[$xsTpe, $keyxTpe] with ResultTypeQueryable[$keyxTpe] {
        override def getKey(..${keyxUDF.params}): $keyxTpe = ${keyxUDF.body}
        override def getProducedType = $keyxTpeInfo
      }
      """
    // generate keyy UDF
    val keyyName = closure.nextUDFName("KeyY")
    closure.closureParams ++= (for (p <- keyyUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $keyyName(..${keyyUDF.closure}) extends org.apache.flink.api.java.functions.KeySelector[$ysTpe, $keyyTpe] with ResultTypeQueryable[$keyyTpe] {
        override def getKey(..${keyyUDF.params}): $keyyTpe = ${keyyUDF.body}
        override def getProducedType = $keyyTpeInfo
      }
      """

    // generate join UDF
    val fName = closure.nextUDFName("Join")
    closure.closureParams ++= (for (p <- fUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $fName(..${fUDF.closure}) extends org.apache.flink.api.common.functions.RichJoinFunction[$xsTpe, $ysTpe, $fTpe] with ResultTypeQueryable[$fTpe] {
        override def join(..${fUDF.params}): $fTpe = ${fUDF.body}
        override def getProducedType = $fTpeInfo
      }
      """

    // assemble dataflow fragment
    q"""
    val __xs = $xs
    val __ys = $ys

    // direct variant
    val keyxSelector = new $keyxName(..${keyxUDF.closure.map(_.name)})
    val keyxType = org.apache.flink.api.java.typeutils.TypeExtractor.getKeySelectorTypes(keyxSelector, __xs.getType)
    val keyx = new org.apache.flink.api.java.operators.Keys.SelectorFunctionKeys[$xsTpe, $keyxTpe](keyxSelector, __xs.getType, keyxType)

    val keyySelector = new $keyyName(..${keyyUDF.closure.map(_.name)})
    val keyyType = org.apache.flink.api.java.typeutils.TypeExtractor.getKeySelectorTypes(keyySelector, __ys.getType)
    val keyy = new org.apache.flink.api.java.operators.Keys.SelectorFunctionKeys[$ysTpe, $keyyTpe](keyySelector, __ys.getType, keyyType)

    val generatedFunction = new org.apache.flink.api.java.operators.JoinOperator.DefaultJoin.WrappingFlatJoinFunction(clean(env, new $fName(..${fUDF.closure.map(_.name)})))

    new org.apache.flink.api.java.operators.JoinOperator.EquiJoin(
      __xs,
      __ys,
      keyx,
      keyy,
      generatedFunction,
      $fTpeInfo,
      org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES,
      "unknown"
    )
    """
  }

  private def opCode[OT, IT1, IT2](op: ir.Cross[OT, IT1, IT2])(implicit closure: DataflowClosure): Tree = {
    val xsTpe = typeOf(op.xs.tag).dealias
    val ysTpe = typeOf(op.ys.tag).dealias

    val f = tb.typecheck(tb.parse(op.f))
    val fUDF = ir.UDF(f, f.tpe.dealias, tb)
    val fTpe = ir.resultType(f.tpe.dealias)
    val fTpeInfo = typeInfoFactory(typeOf(op.tag).dealias)

    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)

    // generate cross UDF
    val fName = closure.nextUDFName("Cross")
    closure.closureParams ++= (for (p <- fUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $fName(..${fUDF.closure}) extends org.apache.flink.api.common.functions.RichCrossFunction[$xsTpe, $ysTpe, $fTpe] with ResultTypeQueryable[$fTpe] {
        override def cross(..${fUDF.params}): $fTpe = ${fUDF.body}
        override def getProducedType = $fTpeInfo
      }
      """

    // assemble dataflow fragment
    q"""
    val __xs = $xs
    val __ys = $ys

    val generatedFunction = clean(env, new $fName(..${fUDF.closure.map(_.name)}))

    new org.apache.flink.api.java.operators.CrossOperator(
      __xs,
      __ys,
      generatedFunction,
      $fTpeInfo,
      org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint.OPTIMIZER_CHOOSES,
      "cross"
    )
    """
  }

  private def opCode[OT, IT](op: ir.Fold[OT, IT])(implicit closure: DataflowClosure): Tree = {
    val srcTpe = typeOf(op.xs.tag).dealias
    val tgtTpe = typeOf(op.tag).dealias
    val tgtTpeInfo = typeInfoFactory(tgtTpe)

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // get fold components
    val sng = tb.typecheck(tb.parse(op.sng))
    val union = tb.typecheck(tb.parse(op.union))

    val mapName = closure.nextUDFName("FoldMapper")
    val mapUDF = ir.UDF(sng, sng.tpe.dealias, tb)
    val mapTpe = tgtTpe
    val mapTpeInfo = tgtTpeInfo // equals to typeInfoFactory(mapTpe)
    closure.closureParams ++= (for (p <- mapUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $mapName(..${mapUDF.closure}) extends org.apache.flink.api.common.functions.RichMapFunction[$srcTpe, $mapTpe] with ResultTypeQueryable[$mapTpe] {
        override def map(..${mapUDF.params}): $mapTpe = ${mapUDF.body}
        override def getProducedType = $mapTpeInfo
      }
      """

    val reduceName = closure.nextUDFName("FoldReducer")
    val reduceUDF = ir.UDF(union, union.tpe.dealias, tb)
    val reduceTpe = tgtTpe
    val reduceTpeInfo = tgtTpeInfo // equals to typeInfoFactory(reduceTpe)
    closure.closureParams ++= (for (p <- reduceUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $reduceName(..${reduceUDF.closure}) extends org.apache.flink.api.common.functions.RichReduceFunction[$reduceTpe] with ResultTypeQueryable[$reduceTpe] {
        override def reduce(..${reduceUDF.params}): $reduceTpe = ${reduceUDF.body}
        override def getProducedType = $reduceTpeInfo
      }
      """

    // assemble dataflow fragment
    q"""
    val __fold = $xs
      .map(new $mapName(..${mapUDF.closure.map(_.name)}))
      .reduce(new $reduceName(..${reduceUDF.closure.map(_.name)}))

    // local collection to store results in
    val __foldCollection = java.util.Collections.synchronizedCollection(new java.util.ArrayList[$tgtTpe]())

    // collect results from remote in local collection
    org.apache.flink.api.java.io.RemoteCollectorImpl.collectLocal(__fold, __foldCollection)

    __foldCollection
    """
  }

  private def opCode[OT, IT](op: ir.FoldGroup[OT, IT])(implicit closure: DataflowClosure): Tree = {
    val srcTpe = typeOf(op.xs.tag).dealias
    val tgtTpe = typeOf(op.tag).dealias
    val tgtTpeInfo = typeInfoFactory(tgtTpe)

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // get fold components
    val key = tb.typecheck(tb.parse(op.key))
    val sng = tb.typecheck(tb.parse(op.sng))
    val sngUDF = ir.UDF(sng, sng.tpe.dealias, tb)
    val union = tb.typecheck(tb.parse(op.union))
    val unionUDF = ir.UDF(union, union.tpe.dealias, tb)

    val mapName = closure.nextUDFName("FoldGroupMapper")
    val mapUDF = {
      val keyUDF = ir.UDF(key, key.tpe.dealias, tb)
      // FIXME: this is not sanitized, input parameters might diverge
      // assemble UDF code
      val udf = tb.typecheck(tb.untypecheck(
        q"""
        () => (x: $srcTpe) => new $tgtTpe(
          ${substitute(keyUDF.params(0).name -> q"x")(keyUDF.body)},
          ${substitute(sngUDF.params(0).name -> q"x")(sngUDF.body)})
        """))
      // construct UDF
      ir.UDF(udf.asInstanceOf[Function], udf.tpe, tb)
    }
    val mapTpe = tgtTpe
    val mapTpeInfo = tgtTpeInfo // equals to typeInfoFactory(mapTpe)
    closure.closureParams ++= (for (p <- mapUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $mapName(..${mapUDF.closure}) extends org.apache.flink.api.common.functions.RichMapFunction[$srcTpe, $mapTpe] with ResultTypeQueryable[$mapTpe] {
        override def map(..${mapUDF.params}): $mapTpe = ${mapUDF.body}
        override def getProducedType = $mapTpeInfo
      }
      """

    val keyName = closure.nextUDFName("FoldGroupKeySelector")
    val keyUDF = {
      // assemble UDF code
      val udf = tb.typecheck(tb.untypecheck(q"() => (x: $tgtTpe) => x.key"))
      // construct UDF
      ir.UDF(udf.asInstanceOf[Function], udf.tpe, tb)
    }
    val keyTpe = keyUDF.body.tpe
    val keyTpeInfo = typeInfoFactory(keyTpe)
    closure.closureParams ++= (for (p <- keyUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $keyName(..${keyUDF.closure}) extends org.apache.flink.api.java.functions.KeySelector[$tgtTpe, $keyTpe] with ResultTypeQueryable[$keyTpe] {
        override def getKey(..${keyUDF.params}): $keyTpe = ${keyUDF.body}
        override def getProducedType = $keyTpeInfo
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
    val reduceTpeInfo = tgtTpeInfo // equals to typeInfoFactory(reduceTpe)
    closure.closureParams ++= (for (p <- reduceUDF.closure) yield p.name -> p.tpt)
    closure.udfs +=
      q"""
      class $reduceName(..${reduceUDF.closure}) extends org.apache.flink.api.common.functions.RichReduceFunction[$reduceTpe] with ResultTypeQueryable[$reduceTpe] {
        override def reduce(..${reduceUDF.params}): $reduceTpe = ${reduceUDF.body}
        override def getProducedType = $reduceTpeInfo
      }
      """

    // assemble dataflow fragment
    q"""
    $xs
      .map(new $mapName(..${mapUDF.closure.map(_.name)}))
      .groupBy(new $keyName(..${keyUDF.closure.map(_.name)}))
      .reduce(new $reduceName(..${reduceUDF.closure.map(_.name)}))
    """
  }

  private def opCode[T](op: ir.Distinct[T])(implicit closure: DataflowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    createTypeConvertor(typeOf(op.tag)) match {
      case tc: ProductTypeConvertor =>
        q"$xs.distinct()"
      case tc: SimpleTypeConvertor =>
        // generate fn UDF
        val fnName = closure.nextUDFName("KeySelector")
        closure.udfs +=
          q"""
          class $fnName extends org.apache.flink.api.java.functions.KeySelector[${tc.tgtType}, ${tc.tgtType}] with ResultTypeQueryable[${tc.tgtType}] {
            override def getKey(v: ${tc.tgtType}): ${tc.tgtType} = v
            override def getProducedType = ${tc.tgtTypeInfo}
          }
          """
        // assemble dataflow fragment
        q"$xs.distinct(new $fnName)"
    }
  }

  private def opCode[T](op: ir.Union[T])(implicit closure: DataflowClosure): Tree = {
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)
    // assemble dataflow fragment
    q"$xs.union($ys)"
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
      // 1) first convert the result type constructor and expand it's parameters
      var tmp = resultTypeConverter.convertResultType(udf.body, udf.params.map(_.symbol).toSet)
      // 2) then convert the parameter projections
      for ((p, ptc) <- udf.params zip paramsTypeConverters) {
        tmp = ptc.convertTermType(p.name, tmp)
      }
      // 3) return
      tmp
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

  private val bagSymbol = dataflowCompiler.mirror.staticClass("eu.stratosphere.emma.api.DataBag")

  private val bagConstructors = bagSymbol.companion.info.decl(TermName("apply")).alternatives

  def substitute(map: (TermName, Tree)*) = new SymbolSubstituter(Map(map: _*))

  class SymbolSubstituter(map: Map[TermName, Tree]) extends Transformer with (Tree => Tree) {
    override def apply(tree: Tree): Tree = transform(tree)

    override def transform(tree: Tree) = tree match {
      case Ident(name@TermName(_)) if map.contains(name) => map(name)
      case _ => super.transform(tree)
    }
  }

}
