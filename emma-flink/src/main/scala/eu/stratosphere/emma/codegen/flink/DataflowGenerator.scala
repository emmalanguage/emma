package eu.stratosphere.emma.codegen.flink

import java.net.URI
import java.util.UUID

import eu.stratosphere.emma.api.{CSVInputFormat, CSVOutputFormat}
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir
import eu.stratosphere.emma.macros.ReflectUtil._
import eu.stratosphere.emma.macros.RuntimeUtil
import eu.stratosphere.emma.runtime.logger
import eu.stratosphere.emma.util.Counter
import org.apache.commons.io.FilenameUtils
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.collection.mutable
import scala.reflect.runtime.universe._

class DataflowGenerator(
      val compiler:  DataflowCompiler,
      val sessionID: UUID = UUID.randomUUID)
    extends RuntimeUtil {

  val tb   = compiler.tb
  val env  = freshName("env$")
  val memo = mutable.Map.empty[String, ModuleSymbol]

  private val tmpDir = "java.io.tmpdir" ->>
    System.getProperty ->>
    FilenameUtils.separatorsToUnix

  // get the path where the toolbox will place temp results
  private val tmpResultsPrefix = s"file:///$tmpDir/emma/temp" ->>
      { System.getProperty("emma.temp.dir", _) } ->>
      { new URI(_) } ->>
      { _.toString }

  private val ExecEnv = typeOf[ExecutionEnvironment]

  private val compile = (t: Tree) => t ->>
    { _.asInstanceOf[ImplDef] } ->>
    { compiler compile _ } ->>
    { _.asModule }

  // --------------------------------------------------------------------------
  // Scatter & Gather DataFlows (hardcoded)
  // --------------------------------------------------------------------------

  def generateScatterDef[A: TypeTag](id: String) = {
    val dataFlowName = s"scatter$$$id"
    memo.getOrElseUpdate(dataFlowName, {
      logger info s"Generating scatter code for '$dataFlowName'"
      val values = freshName("values$")
      // assemble dataFlow
      q"""object ${TermName(dataFlowName)} {
        import _root_.org.apache.flink.api.scala._
        def run($env: $ExecEnv, $values: ${typeOf[Seq[A]]}) =
          $env.fromCollection($values)
      }""" ->> compile
    })
  }

  def generateGatherDef[A: TypeTag](id: String) = {
    val dataFlowName = s"gather$$$id"
    memo.getOrElseUpdate(dataFlowName, {
      logger info s"Generating gather code for '$dataFlowName'"
      val expr = freshName("expr")
      // assemble dataFlow
      q"""object ${TermName(dataFlowName)} {
        import org.apache.flink.api.scala._
        def run($env: $ExecEnv, $expr: ${typeOf[DataSet[A]]}) =
          _root_.eu.stratosphere.emma.api.DataBag($expr.collect)
      }""" ->> compile
    })
  }

  // --------------------------------------------------------------------------
  // Combinator Dataflows (traversal based)
  // --------------------------------------------------------------------------

  def generateDataflowDef(root: ir.Combinator[_], id: String) = {
    val dataFlowName = id
    memo.getOrElseUpdate(dataFlowName, {
      logger info s"Generating dataflow code for '$dataFlowName'"
      // initialize UDF store to be passed around implicitly during dataFlow
      // opCode assembly
      implicit val closure = new DataFlowClosure()
      // generate dataFlow operator assembly code
      val opCode = generateOpCode(root)
      val fn     = freshName("f$")
      val fnTpe  = freshType("F$")

      // create a sorted list of closure parameters
      // (convention used by client in the generated macro)
      val params = for {
        (name, tpt) <- closure.closureParams.toSeq sortBy { _._1.toString }
      } yield q"val $name: $tpt"

      // create a sorted list of closure local inputs
      // (convention used by the Engine client)
      val localInputs = for {
        (name, tpt) <- closure.localInputParams.toSeq sortBy { _._1.toString }
      } yield q"val $name: $tpt"

      val runMethod = root match {
        case op: ir.Fold[_, _] =>
          q"def run($env: $ExecEnv, ..$params, ..$localInputs) = $opCode"

        case op: ir.TempSink[_] =>
          val res = freshName("result$")
          q"""def run($env: $ExecEnv, ..$params, ..$localInputs) = {
            val $res = $opCode
            $env.execute(${s"Emma[$sessionID][$dataFlowName]"})
            $res
          }"""

        case op: ir.Write[_] =>
          q"""def run($env: $ExecEnv, ..$params, ..$localInputs) = {
            $opCode
            $env.execute(${s"Emma[$sessionID][$dataFlowName]"})
            ()
          }"""

        case _ => throw new RuntimeException(
          s"Unsupported root IR node type '${root.getClass}'")
      }

      // assemble dataFlow
      q"""object ${TermName(dataFlowName)} {
        import _root_.org.apache.flink.api.scala._
        ..${closure.UDFs.result().toSeq}
        $runMethod

        def clean[$fnTpe]($env: $ExecEnv, $fn: $fnTpe) = {
          if ($env.getConfig.isClosureCleanerEnabled)
            _root_.org.apache.flink.api.java.ClosureCleaner.clean($fn, true)
          _root_.org.apache.flink.api.java.ClosureCleaner.ensureSerializable($fn)
          $fn
        }
      }""" ->> compile
    })
  }

  private def generateOpCode(combinator: ir.Combinator[_])
      (implicit closure: DataFlowClosure): Tree = combinator match {
    case op: ir.Read[_]           => opCode(op)
    case op: ir.Write[_]          => opCode(op)
    case op: ir.TempSource[_]     => opCode(op)
    case op: ir.TempSink[_]       => opCode(op)
    case op: ir.Scatter[_]        => opCode(op)
    case op: ir.Map[_, _]         => opCode(op)
    case op: ir.FlatMap[_, _]     => opCode(op)
    case op: ir.Filter[_]         => opCode(op)
    case op: ir.EquiJoin[_, _, _] => opCode(op)
    case op: ir.Cross[_, _, _]    => opCode(op)
    case op: ir.Fold[_, _]        => opCode(op)
    case op: ir.FoldGroup[_, _]   => opCode(op)
    case op: ir.Distinct[_]       => opCode(op)
    case op: ir.Union[_]          => opCode(op)
    case op: ir.Group[_, _]       => opCode(op)
    case _ => throw new RuntimeException(
      s"Unsupported ir node of type '${combinator.getClass}'")
  }

  private def opCode[B](op: ir.Read[B])
      (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias

    val inFormatTree = op.format match {
      case fmt: CSVInputFormat[_] =>
        if (!(tpe <:< weakTypeOf[Product]))
          throw new RuntimeException(
            s"Cannot create Flink CsvInputFormat for non-product type ${typeOf(op.tag)}")

        val inf = freshName("inFormat$")

        q"""{
          val $inf =
            new _root_.org.apache.flink.api.scala.operators.ScalaCsvInputFormat[$tpe](
              new _root_.org.apache.flink.core.fs.Path(${op.location}),
              createTypeInformation[$tpe])

          $inf.setFieldDelimiter(${fmt.separator})
          $inf
        }"""

      case _ => throw new RuntimeException(
        s"Unsupported InputFormat of type '${op.format.getClass}'")
    }

    // assemble dataFlow fragment
    q"$env.createInput($inFormatTree)"
  }

  private def opCode[A](op: ir.Write[A])
      (implicit closure: DataFlowClosure): Tree = {
    val tpe = typeOf(op.xs.tag).dealias
    // assemble input fragment
    val xs  = generateOpCode(op.xs)

    val outFormatTree = op.format match {
      case fmt: CSVOutputFormat[_] =>
        if (!(tpe <:< weakTypeOf[Product]))
          throw new RuntimeException(
            s"Cannot create Flink CsvOutputFormat for non-product type $tpe")

        q"""new _root_.org.apache.flink.api.scala.operators.ScalaCsvOutputFormat[$tpe](
            new _root_.org.apache.flink.core.fs.Path(${op.location}),
            ${fmt.separator}.toString)"""

      case _ => throw new RuntimeException(
        s"Unsupported InputFormat of type '${op.format.getClass}'")
    }

    // assemble dataFlow fragment
    q"""$xs.write($outFormatTree, ${op.location},
        _root_.org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)"""
  }

  private def opCode[B](op: ir.TempSource[B])
      (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val tpe   = typeOf(op.tag).dealias
    // add a dedicated closure variable to pass the input param
    val param = TermName(op.ref.name)
    val inf   = freshName("inFormat$")
    closure.closureParams +=
      param -> tq"_root_.eu.stratosphere.emma.runtime.Flink.DataBagRef[$tpe]"
    // assemble dataFlow fragment
    q"""{
      val $inf =
        new org.apache.flink.api.java.io.TypeSerializerInputFormat[$tpe](
          createTypeInformation[$tpe])

      $inf.setFilePath(${s"$tmpResultsPrefix/${op.ref.name}"})
      $env.createInput($inf)
    }"""
  }

  private def opCode[A](op: ir.TempSink[A])
      (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias
    // assemble input fragment
    val xs  = generateOpCode(op.xs)
    val in  = freshName("input$")
    val ti  = freshName("typeInformation$")
    val of  = freshName("outFormat$")
    // assemble dataFlow fragment
    q"""{
      val $in = $xs
      val $ti = createTypeInformation[$tpe]
      val $of = new org.apache.flink.api.java.io.TypeSerializerOutputFormat[$tpe]
      $of.setInputType($ti, $env.getConfig)
      $of.setSerializer($ti.createSerializer($env.getConfig))
      $in.write($of, ${s"$tmpResultsPrefix/${op.name}"},
        _root_.org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)

      $in
    }"""
  }

  private def opCode[B](op: ir.Scatter[B])
      (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias
    // add a dedicated closure variable to pass the scattered term
    val inp = closure.nextLocalInputName()
    closure.localInputParams += inp -> tq"_root_.scala.Seq[$tpe]"
    // assemble dataFlow fragment
    q"$env.fromCollection($inp)"
  }

  private def opCode[B, A](op: ir.Map[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val srcTpe  = typeOf(op.xs.tag).dealias
    val dstTpe  = typeOf(op.tag).dealias
    // assemble input fragment
    val xs      = generateOpCode(op.xs)
    // generate fn UDF
    val mapFn   = parseCheck(op.f)
    val mapUDF  = ir.UDF(mapFn, mapFn.tpe, tb)
    val mapName = closure.nextUDFName("Mapper")
    closure.closureParams ++= mapUDF.closure map { p => p.name -> p.tpt }

    closure.UDFs += q"""class $mapName(..${mapUDF.closure})
        extends _root_.org.apache.flink.api.common.functions.RichMapFunction[$srcTpe, $dstTpe]
        with    _root_.org.apache.flink.api.java.typeutils.ResultTypeQueryable[$dstTpe] {
      override def map(..${mapUDF.params}) = ${mapUDF.body}
      override def getProducedType       = createTypeInformation[$dstTpe]
    }"""

    q"$xs.map(new $mapName(..${mapUDF.closure.map(_.name)}))"
  }

  private def opCode[B, A](op: ir.FlatMap[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val srcTpe = typeOf(op.xs.tag).dealias
    val dstTpe = typeOf(op.tag).dealias
    // assemble input fragment
    val xs     = generateOpCode(op.xs)
    // generate fn UDF
    val fmFn   = parseCheck(op.f)
    val fmUDF  = ir.UDF(fmFn, fmFn.tpe, tb)
    val fmName = closure.nextUDFName("FlatMapper")
    val coll   = freshName("collector$")
    closure.closureParams ++= fmUDF.closure map { p => p.name -> p.tpt }

    closure.UDFs += q"""class $fmName(..${fmUDF.closure})
        extends _root_.org.apache.flink.api.common.functions.RichFlatMapFunction[$srcTpe, $dstTpe]
        with    _root_.org.apache.flink.api.java.typeutils.ResultTypeQueryable[$dstTpe] {
      override def flatMap(..${fmUDF.params},
          $coll: _root_.org.apache.flink.util.Collector[$dstTpe]) =
        ${fmUDF.body}.fetch().foreach($coll.collect)

      override def getProducedType = createTypeInformation[$dstTpe]
    }"""

    // assemble dataFlow fragment
    q"$xs.flatMap(new $fmName(..${fmUDF.closure.map(_.name)}))"
  }

  private def opCode[A](op: ir.Filter[A])
      (implicit closure: DataFlowClosure): Tree = {
    val tpe   = typeOf(op.tag)
    // assemble input fragment
    val xs    = generateOpCode(op.xs)
    // generate fn UDF
    val f     = parseCheck(op.p)
    val fUDF  = ir.UDF(f, f.tpe, tb)
    val fName = closure.nextUDFName("Filter")
    closure.closureParams ++= fUDF.closure map { p => p.name -> p.tpt }

    closure.UDFs += q"""class $fName(..${fUDF.closure})
        extends _root_.org.apache.flink.api.common.functions.RichFilterFunction[$tpe] {
      override def filter(..${fUDF.params}) = ${fUDF.body}
    }"""

    // assemble dataFlow fragment
    q"$xs.filter(new $fName(..${fUDF.closure.map(_.name)}))"
  }

  private def opCode[C, A, B](op: ir.EquiJoin[C, A, B])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)
    val kx = parseCheck(op.keyx).asInstanceOf[Function]
    val ky = parseCheck(op.keyy).asInstanceOf[Function]
    // assemble dataFlow fragment
    q"$xs.join($ys).where(${kx.body}).equalTo(${ky.body})"
  }

  private def opCode[C, A, B](op: ir.Cross[C, A, B])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)
    // assemble dataFlow fragment
    q"$xs.cross($ys)"
  }

  private def opCode[B, A](op: ir.Fold[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    val srcTpe = typeOf(op.xs.tag).dealias
    val dstTpe = typeOf(op.tag).dealias
    // assemble input fragment
    val xs     = generateOpCode(op.xs)
    // get fold components
    val empty  = parseCheck(op.empty)
    val sng    = parseCheck(op.sng)
    val union  = parseCheck(op.union)

    val emptyUDF = ir.UDF(empty, empty.tpe.dealias, tb)
    closure.closureParams ++= emptyUDF.closure map { p => p.name -> p.tpt }

    val mapName = closure nextUDFName "FoldMapper"
    val mapUDF  = ir.UDF(sng, sng.tpe.dealias, tb)
    closure.closureParams ++= mapUDF.closure map { p => p.name -> p.tpt }

    closure.UDFs += q"""class $mapName(..${mapUDF.closure})
        extends _root_.org.apache.flink.api.common.functions.RichMapFunction[$srcTpe, $dstTpe]
        with    _root_.org.apache.flink.api.java.typeutils.ResultTypeQueryable[$dstTpe] {
      override def map(..${mapUDF.params}) = ${mapUDF.body}
      override def getProducedType         = createTypeInformation[$dstTpe]
    }"""

    val foldName = closure.nextUDFName("FoldReducer")
    val foldUDF  = ir.UDF(union, union.tpe.dealias, tb)
    closure.closureParams ++= foldUDF.closure map { p => p.name -> p.tpt }

    closure.UDFs += q"""class $foldName(..${foldUDF.closure})
        extends _root_.org.apache.flink.api.common.functions.RichReduceFunction[$dstTpe]
        with    _root_.org.apache.flink.api.java.typeutils.ResultTypeQueryable[$dstTpe] {
      override def reduce(..${foldUDF.params}) = ${foldUDF.body}
      override def getProducedType = createTypeInformation[$dstTpe]
    }"""

    val result = freshName("result$")
    // assemble dataFlow fragment
    q"""{
      val $result = $xs
        .map(new $mapName(..${mapUDF.closure.map(_.name)}))
        .reduce(new $foldName(..${foldUDF.closure.map(_.name)}))
        .collect

      if ($result.isEmpty) ${empty.asInstanceOf[Function].body}
      else $result.head
    }"""
  }

  private def opCode[B, A](op: ir.FoldGroup[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    val srcTpe   = typeOf(op.xs.tag).dealias
    val dstTpe   = typeOf(op.tag).dealias
    // assemble input fragment
    val xs       = generateOpCode(op.xs)
    // get fold components
    val key      = parseCheck(op.key)
    val keyUDF   = ir.UDF(key, key.tpe.dealias, tb)
    val empty    = parseCheck(op.empty)
    val emptyUDF = ir.UDF(empty, empty.tpe.dealias, tb)
    val sng      = parseCheck(op.sng)
    val sngUDF   = ir.UDF(sng, sng.tpe.dealias, tb)
    val union    = parseCheck(op.union)
    val unionUDF = ir.UDF(union, union.tpe.dealias, tb)
    val mapName  = closure nextUDFName "FoldGroupMapper"
    val extract  = freshName("extractKey$")
    val mapAgg   = freshName("mapAggregates$")
    val x        = freshName("x$")

    closure.UDFs += q"""class $mapName
        extends _root_.org.apache.flink.api.common.functions.RichMapFunction[$srcTpe, $dstTpe]
        with    _root_.org.apache.flink.api.java.typeutils.ResultTypeQueryable[$dstTpe] {
      def $extract(..${keyUDF.params}) = ${keyUDF.body}
      def $mapAgg( ..${sngUDF.params}) = ${sngUDF.body}
      override def getProducedType     = createTypeInformation[$dstTpe]
      override def map($x: $srcTpe)    = new $dstTpe($extract($x), $mapAgg($x))
    }"""

    val foldName = closure nextUDFName "FoldGroupReducer"
    val foldUDF  = {
      val x   = freshName("x$")
      val y   = freshName("y$")
      // assemble UDF code
      val udf = reTypeCheck(q"""() => ($x: $dstTpe, $y: $dstTpe) => {
          val ${unionUDF.params.head.name} = $x.values
          val ${unionUDF.params( 1 ).name} = $y.values
          new $dstTpe($x.key, ${unionUDF.body})
        }""")
      // construct UDF
      ir.UDF(udf.asInstanceOf[Function], udf.tpe, tb)
    }
    // add closure parameters
    closure.closureParams ++=   keyUDF.closure map { p => p.name -> p.tpt }
    closure.closureParams ++= emptyUDF.closure map { p => p.name -> p.tpt }
    closure.closureParams ++=   sngUDF.closure map { p => p.name -> p.tpt }
    closure.closureParams ++=  foldUDF.closure map { p => p.name -> p.tpt }

    closure.UDFs +=q"""class $foldName(..${foldUDF.closure})
        extends _root_.org.apache.flink.api.common.functions.RichReduceFunction[$dstTpe]
        with    _root_.org.apache.flink.api.java.typeutils.ResultTypeQueryable[$dstTpe] {
      override def reduce(..${foldUDF.params}) = ${foldUDF.body}
      override def getProducedType = createTypeInformation[$dstTpe]
    }"""

    // assemble dataFlow fragment
    q"""$xs.map(new $mapName()).groupBy(_.key).reduce(
        new $foldName(..${foldUDF.closure.map(_.name)}))"""
  }

  private def opCode[A](op: ir.Distinct[A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // assemble dataFlow fragment
    q"$xs.distinct(x => x)"
  }

  private def opCode[A](op: ir.Union[A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)
    // assemble dataFlow fragment
    q"$xs.union($ys)"
  }

  private def opCode[B, A](op: ir.Group[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    val srcTpe   = typeOf(op.xs.tag).dealias
    // assemble input fragment
    val xs       = generateOpCode(op.xs)
    val iterator = freshName("iterator$")
    val stream   = freshName("stream$")
    // generate key UDF
    val keyFn    = parseCheck(op.key)
    val keyUDF   = ir.UDF(keyFn, keyFn.tpe, tb)
    closure.closureParams ++= keyUDF.closure map { p => p.name -> p.tpt }
    // assemble dataFlow fragment
    q"""$xs.groupBy({ (..${keyUDF.params}) =>
        ${keyUDF.body}
      }).reduceGroup({ ($iterator: _root_.scala.Iterator[$srcTpe]) =>
        val $stream = $iterator.toStream
        _root_.eu.stratosphere.emma.api.Group(
          ((..${keyUDF.params}) => ${keyUDF.body})($stream.head),
          _root_.eu.stratosphere.emma.api.DataBag($stream))
      })"""
  }

  // --------------------------------------------------------------------------
  // Auxiliary structures
  // --------------------------------------------------------------------------

  private class DataFlowClosure(
      udfCounter:     Counter = new Counter(),
      scatterCounter: Counter = new Counter()) {
    val closureParams    = mutable.Map.empty[TermName, Tree]
    val localInputParams = mutable.Map.empty[TermName, Tree]
    val UDFs             = mutable.ArrayBuilder.make[Tree]

    def nextUDFName(prefix: String = "UDF") =
      TypeName(f"Emma$prefix${udfCounter.advance.get}%05d")

    def nextLocalInputName(prefix: String = "scatter") =
      TermName(f"scatter${scatterCounter.advance.get}%05d")
  }
}
