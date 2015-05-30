package eu.stratosphere.emma.codegen.spark

import java.util.UUID

import eu.stratosphere.emma.api.{CSVOutputFormat, CSVInputFormat}
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir
import eu.stratosphere.emma.macros.ReflectUtil._
import eu.stratosphere.emma.macros.RuntimeUtil
import eu.stratosphere.emma.runtime.logger
import eu.stratosphere.emma.util.Counter
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.reflect.runtime.universe._


class DataflowGenerator(
      val compiler:  DataflowCompiler,
      val sessionID: UUID = UUID.randomUUID())
    extends RuntimeUtil {

  val tb   = compiler.tb
  val sc   = freshName("sc$")
  val memo = mutable.Map[String, ModuleSymbol]()

  private val SparkCtx = typeOf[SparkContext]

  private val compile  = (t: Tree) => t ->>
    { _.asInstanceOf[ImplDef] } ->>
    { compiler compile _ } ->>
    { _.asModule }

  // --------------------------------------------------------------------------
  // Combinator DataFlows (traversal based)
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

      // create a sorted list of closure parameters
      // (convention used by client in the generated macro)
      val params = for {
        (name, tpt) <- closure.closureParams.toSeq sortBy { _._1.toString }
      } yield q"val $name: $tpt"

      // create a sorted list of closure local inputs
      // (convention used by the Engine client)
      val localInputs = for {
        (name, tpt) <- closure.localInputParams.toSeq sortBy { _._1.toString }
      } yield q"$name: $tpt"

      val runMethod = root match {
        case op: ir.Fold[_, _] =>
          q"def run($sc: $SparkCtx, ..$params, ..$localInputs) = $opCode"

        case op: ir.Write[_] =>
          q"def run($sc: $SparkCtx, ..$params, ..$localInputs) = $opCode"

        case op: ir.Combinator[_] =>
          val res = freshName("result$")
          q"""def run($sc: $SparkCtx, ..$params, ..$localInputs) = {
            val $res = $opCode.cache()
            $res.foreach(_ => ())
            $res
          }"""
      }

      // assemble dataFlow
      q"""object ${TermName(dataFlowName)} {
        import _root_.org.apache.spark.SparkContext._
        ..${closure.UDFs.result().toSeq}
        $runMethod
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
    val tpe  = typeOf(op.tag).dealias
    val path = op.location

    op.format match {
      case fmt: CSVInputFormat[tp] =>
        if (!(tpe <:< weakTypeOf[Product]))
          throw new RuntimeException(
            s"Cannot create Flink CsvInputFormat for non-product type ${typeOf(op.tag)}")

        val line = freshName("line$")
        val name = closure nextTermName "converter"
        closure.UDFs += q"""val $name =
          _root_.eu.stratosphere.emma.api.`package`.materializeCSVConvertors[$tpe]"""

        q"""$sc.textFile($path).map({ ($line: ${typeOf[String]}) =>
          $name.fromCSV($line.split(${fmt.separator}))
        })"""

      case _ => throw new RuntimeException(
        s"Unsupported InputFormat of type '${op.format.getClass}'")
    }
  }

  private def opCode[A](op: ir.Write[A])
      (implicit closure: DataFlowClosure): Tree = {
    val tpe = typeOf(op.xs.tag).dealias
    // assemble input fragment
    val xs  = generateOpCode(op.xs)

    val outFormatTree = op.format match {
      case fmt: CSVOutputFormat[_] =>
        val sep = fmt.separator.toString
        val e   = freshName("e$")
        if (tpe <:< weakTypeOf[Product])
          q"""$xs.map({ case $e =>
            0.until($e.productArity).map($e.productElement(_)).mkString($sep)
          })"""
        else throw new RuntimeException(
          s"Cannot create Spark CsvOutputFormat for non-product type ${typeOf(op.tag)}")

      case _ => throw new RuntimeException(
        s"Unsupported OutputFormat of type '${op.format.getClass}'")
    }

    // assemble dataFlow fragment
    q"$outFormatTree.saveAsTextFile(${op.location})"
  }

  private def opCode[B](op: ir.TempSource[B])
      (implicit closure: DataFlowClosure): Tree = {
    val tpe   = typeOf(op.tag).dealias
    // add a dedicated closure variable to pass the input param
    val param = TermName(op.ref.name)
    closure.closureParams +=
      param -> tq"_root_.eu.stratosphere.emma.runtime.Spark.DataBagRef[$tpe]"

    q"$param.rdd"
  }

  private def opCode[A](op: ir.TempSink[A])
      (implicit closure: DataFlowClosure): Tree =
    generateOpCode(op.xs)

  private def opCode[B](op: ir.Scatter[B])
      (implicit closure: DataFlowClosure): Tree = {
    val tpe = typeOf(op.tag).dealias
    // add a dedicated closure variable to pass the scattered term
    val ip  = closure.nextTermName()
    closure.localInputParams += ip -> tq"_root_.scala.Seq[$tpe]"
    q"$sc.parallelize($ip)"
  }

  private def opCode[OT, IT](op: ir.Map[OT, IT])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs     = generateOpCode(op.xs)
    // generate fn UDF
    val mapFn  = parseCheck(op.f)
    val mapUDF = ir.UDF(mapFn, mapFn.tpe, tb)
    closure.closureParams ++= mapUDF.closure map { p => p.name -> p.tpt }
    // assemble dataFlow fragment
    q"$xs.map((..${mapUDF.params}) => ${mapUDF.body})"
  }

  private def opCode[B, A](op: ir.FlatMap[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs    = generateOpCode(op.xs)
    // generate fn UDF
    val fmFn  = parseCheck(op.f)
    val fmUDF = ir.UDF(fmFn, fmFn.tpe, tb)
    closure.closureParams ++= fmUDF.closure map { p => p.name -> p.tpt }
    q"$xs.flatMap((..${fmUDF.params}) => ${fmUDF.body}.fetch())"
  }


  private def opCode[A](op: ir.Filter[A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs   = generateOpCode(op.xs)
    // generate fn UDF
    val f    = parseCheck(op.p)
    val fUDF = ir.UDF(f, f.tpe, tb)
    closure.closureParams ++= fUDF.closure map { p => p.name -> p.tpt }
    // assemble dataflow fragment
    q"$xs.filter((..${fUDF.params}) => ${fUDF.body})"
  }

  private def opCode[C, A, B](op: ir.EquiJoin[C, A, B])
      (implicit closure: DataFlowClosure): Tree = {
    val kx    = parseCheck(op.keyx)
    val kxUDF = ir.UDF(kx, kx.tpe.dealias, tb)
    val ky    = parseCheck(op.keyy)
    val kyUDF = ir.UDF(ky, ky.tpe.dealias, tb)
    val f     = parseCheck(op.f)
    val fUDF  = ir.UDF(f, f.tpe.dealias, tb)
    // assemble input fragments
    val xs    = generateOpCode(op.xs)
    val ys    = generateOpCode(op.ys)
    val left  = freshName("xs$")
    val right = freshName("ys$")
    // generate kx UDF
    closure.closureParams ++= kxUDF.closure map { p => p.name -> p.tpt }
    // generate ky UDF
    closure.closureParams ++= kyUDF.closure map { p => p.name -> p.tpt }
    // generate join UDF
    closure.closureParams ++=  fUDF.closure map { p => p.name -> p.tpt }
    // assemble dataflow fragment
    q"""val $left  = $xs.map({ (..${kxUDF.params}) =>
        (${kxUDF.body}, ${kxUDF.params.head.name})
      })

      val $right = $ys.map({ (..${kyUDF.params}) =>
        (${kyUDF.body}, ${kyUDF.params.head.name})
      })

      $left.join($right).map(_._2)"""
  }

  private def opCode[C, A, B](op: ir.Cross[C, A, B])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)
    // assemble dataFlow fragment
    q"$xs.cartesian($ys)"
  }

  private def opCode[B, A](op: ir.Fold[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs       = generateOpCode(op.xs)
    // get fold components
    val sng      = parseCheck(op.sng)
    val sngUDF   = ir.UDF(sng, sng.tpe.dealias, tb)
    val union    = parseCheck(op.union)
    val unionUDF = ir.UDF(union, union.tpe.dealias, tb)
    // assemble dataFlow fragment
    q"""$xs.map((..${sngUDF.params}) => ${sngUDF.body})
        .reduce((..${unionUDF.params}) => ${unionUDF.body})"""
  }

  private def opCode[B, A](op: ir.FoldGroup[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs       = generateOpCode(op.xs)
    // get fold components
    val key      = parseCheck(op.key)
    val keyUDF   = ir.UDF(key, key.tpe.dealias, tb)
    val sng      = parseCheck(op.sng)
    val sngUDF   = ir.UDF(sng, sng.tpe.dealias, tb)
    val union    = parseCheck(op.union)
    val unionUDF = ir.UDF(union, union.tpe.dealias, tb)
    // assemble dataFlow fragment
    closure.closureParams ++=   keyUDF.closure map { p => p.name -> p.tpt }
    closure.closureParams ++=   sngUDF.closure map { p => p.name -> p.tpt }
    closure.closureParams ++= unionUDF.closure map { p => p.name -> p.tpt }

    val aliases = for {
      (alias, origin) <- sngUDF.params zip keyUDF.params
      (aName, oName )  = (alias.name, origin.name)
      if aName != oName
    } yield aName -> Ident(oName)

    val x = freshName("x$")
    val y = freshName("y$")

    q"""$xs.map({ (..${keyUDF.params}) =>
        (${keyUDF.body}, ${bind(sngUDF.body, aliases: _*)})
      }).reduceByKey({ (..${unionUDF.params}) =>
        ${unionUDF.body}
      }).map({ case ($x, $y) =>
        _root_.eu.stratosphere.emma.api.Group($x, $y)
      })"""
  }

  private def opCode[A](op: ir.Distinct[A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    q"$xs.distinct()"
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
    val key      = freshName("key$")
    val iterator = freshName("iterator$")
    // assemble input fragment
    val xs       = generateOpCode(op.xs)
    // generate key UDF
    val keyFn    = parseCheck(op.key)
    val keyUDF   = ir.UDF(keyFn, keyFn.tpe, tb)
    closure.closureParams ++= keyUDF.closure map { p => p.name -> p.tpt }
    // assemble dataFlow fragment
    q"""$xs.groupBy({ (..${keyUDF.params}) =>
        ${keyUDF.body}
      }).map({ case ($key, $iterator) =>
        _root_.eu.stratosphere.emma.api.Group($key,
          _root_.eu.stratosphere.emma.api.DataBag($iterator.toStream))
      })"""
  }

  // --------------------------------------------------------------------------
  // Auxiliary structures
  // --------------------------------------------------------------------------

  private final class DataFlowClosure(
      udfCounter:     Counter = new Counter(),
      scatterCounter: Counter = new Counter()) {
    val closureParams    = mutable.Map.empty[TermName, Tree]
    val localInputParams = mutable.Map.empty[TermName, Tree]
    val UDFs             = mutable.ArrayBuilder.make[Tree]

    def nextUDFName(prefix: String = "UDF") =
      TypeName(f"Emma$prefix${udfCounter.advance.get}%05d")

    def nextTermName(prefix: String = "scatter") =
      TermName(f"$prefix${scatterCounter.advance.get}%05d")
  }
}
