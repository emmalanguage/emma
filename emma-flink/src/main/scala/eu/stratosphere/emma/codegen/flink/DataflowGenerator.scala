package eu.stratosphere.emma.codegen.flink

import java.net.URI
import java.util.UUID

import eu.stratosphere.emma.api.{ParallelizedDataBag, TextInputFormat, CSVInputFormat, CSVOutputFormat}
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir
import eu.stratosphere.emma.macros.RuntimeUtil
import eu.stratosphere.emma.runtime.{StatefulBackend, logger}
import org.apache.commons.io.FilenameUtils
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.collection.mutable
import scala.reflect.runtime.universe._

class DataflowGenerator(
      val compiler:  DataflowCompiler,
      val sessionID: UUID = UUID.randomUUID)
    extends RuntimeUtil {

  val tb   = compiler.tb
  val env  = freshName("env$flink$")
  val memo = mutable.Map.empty[String, ModuleSymbol]

  private val tmpDir =
    "java.io.tmpdir" ->>
    System.getProperty ->>
    FilenameUtils.separatorsToUnix

  // get the path where the toolbox will place temp results
  private val tmpResultsPrefix =
    new URI(System.getProperty("emma.temp.dir", s"file:///$tmpDir/emma/temp")).toString

  private val ExecEnv = typeOf[ExecutionEnvironment]

  private val compile = (t: Tree) =>
    compiler.compile(t.asInstanceOf[ImplDef]).asModule

  // --------------------------------------------------------------------------
  // Scatter & Gather DataFlows (hardcoded)
  // --------------------------------------------------------------------------

  def generateScatterDef[A: TypeTag](id: String) = {
    val dataFlowName = s"scatter$$$id"
    memo.getOrElseUpdate(dataFlowName, {
      logger info s"Generating scatter code for '$dataFlowName'"
      val vals = freshName("vals$flink$")
      // assemble dataFlow
      q"""object ${TermName(dataFlowName)} {
        import _root_.org.apache.flink.api.scala._
        def run($env: $ExecEnv, $vals: ${typeOf[Seq[A]]}) =
          $env.fromCollection($vals)
      }""" ->> compile
    })
  }

  def generateGatherDef[A: TypeTag](id: String) = {
    val dataFlowName = s"gather$$$id"
    memo.getOrElseUpdate(dataFlowName, {
      logger info s"Generating gather code for '$dataFlowName'"
      val expr = freshName("expr$flink$")
      // assemble dataFlow
      q"""object ${TermName(dataFlowName)} {
        import _root_.org.apache.flink.api.scala._
        def run($env: $ExecEnv, $expr: ${typeOf[DataSet[A]]}) =
          _root_.eu.stratosphere.emma.api.DataBag($expr.collect())
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
      val fn     = freshName("f$flink$")
      val fnTpe  = freshType("F$flink$")

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
          val res = freshName("res$flink$")
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

        // todo: this won't work if there are multiple statefuls in one comprehension
        case op: ir.StatefulCreate[_,_] =>
          val res = freshName("res$flink$")
          q"""def run($env: $ExecEnv, ..$params, ..$localInputs) = {
            val $res = $opCode
            $env.execute(${s"Emma[$sessionID][$dataFlowName]"})
            $res
          }"""

        case _ => throw new RuntimeException(
          s"Unsupported root IR node type '${root.getClass}'")
      }

      // assemble dataFlow
      q"""object ${TermName(dataFlowName)} {
        import _root_.org.apache.flink.api.scala._

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
    case op: ir.Read[_]                    => opCode(op)
    case op: ir.Write[_]                   => opCode(op)
    case op: ir.TempSource[_]              => opCode(op)
    case op: ir.TempSink[_]                => opCode(op)
    case op: ir.Scatter[_]                 => opCode(op)
    case op: ir.Map[_, _]                  => opCode(op)
    case op: ir.FlatMap[_, _]              => opCode(op)
    case op: ir.Filter[_]                  => opCode(op)
    case op: ir.EquiJoin[_, _, _]          => opCode(op)
    case op: ir.Cross[_, _, _]             => opCode(op)
    case op: ir.Fold[_, _]                 => opCode(op)
    case op: ir.FoldGroup[_, _]            => opCode(op)
    case op: ir.Distinct[_]                => opCode(op)
    case op: ir.Union[_]                   => opCode(op)
    case op: ir.Group[_, _]                => opCode(op)
    case op: ir.StatefulCreate[_, _]       => opCode(op)
    case op: ir.StatefulFetch[_, _]        => opCode(op)
    case op: ir.UpdateWithZero[_, _, _]    => opCode(op)
    case op: ir.UpdateWithOne[_, _, _, _]  => opCode(op)
    case op: ir.UpdateWithMany[_, _, _, _] => opCode(op)
    case _ => throw new RuntimeException(
      s"Unsupported ir node of type '${combinator.getClass}'")
  }

  private def opCode[B](op: ir.Read[B])
      (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias

    val inFormatTree = op.format match {
      case fmt: TextInputFormat[_] =>

        val inf = freshName("inFormat$flink$")

        q"""{
          val $inf =
            new _root_.org.apache.flink.api.java.io.TextInputFormat(
              new _root_.org.apache.flink.core.fs.Path(${op.location}))

          $inf.setDelimiter(${fmt.separator})
          $inf
        }"""

      case fmt: CSVInputFormat[_] =>
        if (!(tpe <:< weakTypeOf[Product]))
          throw new RuntimeException(
            s"Cannot create Flink CsvInputFormat for non-product type ${typeOf(op.tag)}")

        val inf = freshName("inFmt$flink$")

        q"""{
          val $inf =
            new _root_.org.apache.flink.api.scala.operators.ScalaCsvInputFormat[$tpe](
              new _root_.org.apache.flink.core.fs.Path(${op.location}),
              _root_.org.apache.flink.api.scala.createTypeInformation[$tpe])

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
    val param = TermName(op.ref.asInstanceOf[ParallelizedDataBag[B, DataSet[B]]].name)
    val inf   = freshName("inFmt$flink$")
    closure.closureParams +=
      param -> tq"_root_.eu.stratosphere.emma.api.DataBag[$tpe]"
    // assemble dataFlow fragment
    q"""{
      val $inf =
        new org.apache.flink.api.java.io.TypeSerializerInputFormat[$tpe](
          _root_.org.apache.flink.api.scala.createTypeInformation[$tpe])

      $inf.setFilePath(${s"$tmpResultsPrefix/${op.ref.asInstanceOf[ParallelizedDataBag[B, DataSet[B]]].name}"})
      $env.createInput($inf)
    }"""
  }

  private def opCode[A](op: ir.TempSink[A])
      (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val tpe = typeOf(op.tag).dealias
    // assemble input fragment
    val xs  = generateOpCode(op.xs)
    val in  = freshName("input$flink$")
    val ti  = freshName("typeInfo$flink$")
    val of  = freshName("outFmt$flink$")
    // assemble dataFlow fragment
    q"""{
      val $in = $xs
      val $ti = _root_.org.apache.flink.api.scala.createTypeInformation[$tpe]
      val $of = new _root_.org.apache.flink.api.java.io.TypeSerializerOutputFormat[$tpe]
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
    val inp = freshName("scatter$flink$")
    closure.localInputParams += inp -> tq"_root_.scala.Seq[$tpe]"
    // assemble dataFlow fragment
    q"$env.fromCollection($inp)"
  }

  private def opCode[B, A](op: ir.Map[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs     = generateOpCode(op.xs)
    // generate fn UDF
    val mapFn  = parseCheck(op.f)
    val mapUDF = ir.UDF(mapFn, mapFn.tpe, tb)
    closure.capture(mapUDF)
    q"$xs.map(${mapUDF.func})"
  }

  private def opCode[B, A](op: ir.FlatMap[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs    = generateOpCode(op.xs)
    // generate fn UDF
    val fmFn  = parseCheck(op.f)
    val fmUDF = ir.UDF(fmFn, fmFn.tpe, tb)
    closure.capture(fmUDF)
    // assemble dataFlow fragment
    q"$xs.flatMap((..${fmUDF.params}) => ${fmUDF.body}.fetch())"
  }

  private def opCode[A](op: ir.Filter[A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs   = generateOpCode(op.xs)
    // generate fn UDF
    val f    = parseCheck(op.p)
    val fUDF = ir.UDF(f, f.tpe, tb)
    closure.capture(fUDF)
    // assemble dataFlow fragment
    q"$xs.filter(${fUDF.func})"
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
    // assemble input fragment
    val xs       = generateOpCode(op.xs)
    // get fold components
    val empty    = parseCheck(op.empty)
    val sng      = parseCheck(op.sng)
    val union    = parseCheck(op.union)
    // create UDFs
    val emptyUDF = ir.UDF(empty, empty.tpe.dealias, tb)
    val mapUDF   = ir.UDF(sng,   sng.tpe.dealias,   tb)
    val foldUDF  = ir.UDF(union, union.tpe.dealias, tb)
    // capture closures
    closure.capture(emptyUDF, mapUDF, foldUDF)
    val res = freshName("res$flink$")
    // assemble dataFlow fragment
    q"""{
      val $res = $xs.map(${mapUDF.func}).reduce(${foldUDF.func}).collect()
      if ($res.isEmpty) ${empty.asInstanceOf[Function].body} else $res.head
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
    val x        = freshName("x$flink$")
    val y        = freshName("y$flink$")
    val mapFn    = q"($x: $srcTpe) => new $dstTpe(${keyUDF.func}($x), ${sngUDF.func}($x))"
    val foldUDF  = {
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
    closure.capture(keyUDF, emptyUDF, sngUDF, foldUDF)
    // assemble dataFlow fragment
    q"$xs.map($mapFn).groupBy(_.key).reduce(${foldUDF.func})"
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
    val iterator = freshName("iter$flink$")
    val stream   = freshName("stream$flink$")
    // generate key UDF
    val keyFn    = parseCheck(op.key)
    val keyUDF   = ir.UDF(keyFn, keyFn.tpe, tb)
    closure.capture(keyUDF)
    // assemble dataFlow fragment
    q"""$xs.groupBy(${keyUDF.func}).reduceGroup({
        ($iterator: _root_.scala.Iterator[$srcTpe]) =>
          val $stream = $iterator.toStream
          _root_.eu.stratosphere.emma.api.Group(
            ${keyUDF.func}($stream.head),
            _root_.eu.stratosphere.emma.api.DataBag($stream))
      })"""
  }

  private def opCode[A, K](op: ir.StatefulCreate[A, K])
      (implicit closure: DataFlowClosure): Tree = {
    val stateType = typeOf(op.tagS).dealias
    val keyType = typeOf(op.tagK).dealias
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    q"""
      import _root_.eu.stratosphere.emma.runtime.StatefulBackend
      new StatefulBackend[$stateType,$keyType]($env,$xs)"""
  }

  private def opCode[S, K](op: ir.StatefulFetch[S, K])
      (implicit closure: DataFlowClosure): Tree = {
    closure.closureParams += TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).dealias)
    q"""${TermName(op.name)}
        .asInstanceOf[_root_.eu.stratosphere.emma.runtime.StatefulBackend[${typeOf(op.tag).dealias}, ${typeOf(op.tagK).dealias}]]
        .fetchToStateLess()"""
  }

  private def opCode[S, K, U, O](op: ir.UpdateWithZero[S, K, O])
      (implicit closure: DataFlowClosure): Tree = {
    closure.closureParams += TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).dealias)
    val updFn  = parseCheck(op.udf)
    val updUdf = ir.UDF(updFn, updFn.tpe, tb)
    val updUdfOutTpe = typeOf(op.tag).dealias
    closure.capture(updUdf)
    q"""${TermName(op.name)}
        .asInstanceOf[_root_.eu.stratosphere.emma.runtime.StatefulBackend[${typeOf(op.tagS).dealias}, ${typeOf(op.tagK).dealias}]]
        .updateWithZero[$updUdfOutTpe](${updUdf.func})"""
  }

  private def opCode[S, K, U, O](op: ir.UpdateWithOne[S, K, U, O])
      (implicit closure: DataFlowClosure): Tree = {
    closure.closureParams += TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).dealias)
    val us = generateOpCode(op.updates)
    val keyFn     = parseCheck(op.updateKeySel)
    val keyUdf    = ir.UDF(keyFn, keyFn.tpe, tb)
    val updateFn  = parseCheck(op.udf)
    val updateUdf = ir.UDF(updateFn, updateFn.tpe, tb)
    val updTpe = typeOf(op.tagU).dealias
    val updUdfOutTpe = typeOf(op.tag).dealias
    closure.capture(keyUdf)
    closure.capture(updateUdf)
    q"""${TermName(op.name)}.
        asInstanceOf[_root_.eu.stratosphere.emma.runtime.StatefulBackend[${typeOf(op.tagS).dealias}, ${typeOf(op.tagK).dealias}]]
        .updateWithOne[$updTpe, $updUdfOutTpe]($us, ${keyUdf.func}, ${updateUdf.func})"""
  }

  private def opCode[S, K, U, O](op: ir.UpdateWithMany[S, K, U, O])
      (implicit closure: DataFlowClosure): Tree = {
    closure.closureParams += TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).dealias)
    val us = generateOpCode(op.updates)
    val keyFn     = parseCheck(op.updateKeySel)
    val keyUdf    = ir.UDF(keyFn, keyFn.tpe, tb)
    val updateFn  = parseCheck(op.udf)
    val updateUdf = ir.UDF(updateFn, updateFn.tpe, tb)
    val updTpe = typeOf(op.tagU).dealias
    val updUdfOutTpe = typeOf(op.tag).dealias
    closure.capture(keyUdf)
    closure.capture(updateUdf)
    q"""${TermName(op.name)}
        .asInstanceOf[_root_.eu.stratosphere.emma.runtime.StatefulBackend[${typeOf(op.tagS).dealias}, ${typeOf(op.tagK).dealias}]]
        .updateWithMany[$updTpe, $updUdfOutTpe]($us, ${keyUdf.func}, ${updateUdf.func})"""
  }

  // --------------------------------------------------------------------------
  // Auxiliary structures
  // --------------------------------------------------------------------------

  private class DataFlowClosure {
    val closureParams    = mutable.Map.empty[TermName, Tree]
    val localInputParams = mutable.Map.empty[TermName, Tree]

    def capture(fs: ir.UDF*) = for (f <- fs)
      closureParams ++= f.closure map { p => p.name -> p.tpt }
  }
}
