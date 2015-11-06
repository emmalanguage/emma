package eu.stratosphere.emma.codegen.spark

import java.util.UUID

import eu.stratosphere.emma.api.{ParallelizedDataBag, CSVOutputFormat, CSVInputFormat, TextInputFormat}
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir
import eu.stratosphere.emma.macros.RuntimeUtil
import eu.stratosphere.emma.runtime.logger

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.runtime.universe._

class DataflowGenerator(val compiler: DataflowCompiler, val sessionID: UUID = UUID.randomUUID())
    extends RuntimeUtil {

  import syntax._
  val tb = compiler.tb
  val sc = $"sc$$spark"
  val memo = mutable.Map[String, ModuleSymbol]()

  private val SPARK_CTX = typeOf[SparkContext]
  private val compile = (tree: Tree) => tree ->>
    { _.as[ImplDef] } ->>
    { compiler compile _ } ->>
    { _.asModule }

  // --------------------------------------------------------------------------
  // Combinator DataFlows (traversal based)
  // --------------------------------------------------------------------------

  def generateDataflowDef(root: ir.Combinator[_], id: String) = {
    val dataFlowName = id
    memo.getOrElseUpdate(dataFlowName, {
      logger.info("Generating dataflow code for '{}'", dataFlowName)
      // initialize UDF store to be passed around implicitly during dataFlow
      // opCode assembly
      implicit val closure = new DataFlowClosure
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
          q"def run($sc: $SPARK_CTX, ..$params, ..$localInputs) = $opCode"

        case op: ir.Write[_] =>
          q"def run($sc: $SPARK_CTX, ..$params, ..$localInputs) = $opCode"

        case op: ir.StatefulCreate[_, _] =>
          q"def run($sc: $SPARK_CTX, ..$params, ..$localInputs) = $opCode"

        case op: ir.Combinator[_] =>
          val result = $"result$$spark"
          q"""def run($sc: $SPARK_CTX, ..$params, ..$localInputs) = {
            val $result = $opCode.cache()
            $result.foreach(_ => ())
            $result
          }"""

      }

      // assemble dataFlow
      q"""object ${TermName(dataFlowName)} {
        import _root_.org.apache.spark.SparkContext._
        $runMethod
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
    val tpe = typeOf(op.tag).precise
    val path = op.location

    op.format match {
      case fmt: CSVInputFormat[tp] =>
        if (!(tpe <:< weakTypeOf[Product]))
          throw new RuntimeException(
            s"Cannot create Spark CsvInputFormat for non-product type ${typeOf(op.tag)}")

        val line = $"line$$spark"
        q"""$sc.textFile($path).map({ ($line: ${typeOf[String]}) =>
          _root_.eu.stratosphere.emma.api.`package`.materializeCSVConverters[$tpe]
            .fromCSV($line.split(${fmt.separator}))
        })"""

      case fmt: TextInputFormat[tp] =>
        q"""$sc.textFile($path)"""

      case _ => throw new RuntimeException(
        s"Unsupported InputFormat of type '${op.format.getClass}'")
    }
  }

  private def opCode[A](op: ir.Write[A])
      (implicit closure: DataFlowClosure): Tree = {
    val tpe = typeOf(op.xs.tag).precise
    // assemble input fragment
    val xs = generateOpCode(op.xs)

    val outFormatTree = op.format match {
      case fmt: CSVOutputFormat[_] =>
        val sep = fmt.separator.toString
        val el = $"el$$spark"
        if (tpe <:< weakTypeOf[Product])
          q"""$xs.map({ case $el =>
            0.until($el.productArity).map($el.productElement(_)).mkString($sep)
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
    val tpe = typeOf(op.tag).precise
    // add a dedicated closure variable to pass the input param
    val param = TermName(op.ref.asInstanceOf[ParallelizedDataBag[B, RDD[B]]].name)
    closure.closureParams += param -> tq"_root_.eu.stratosphere.emma.api.DataBag[$tpe]"
    q"""$param.asInstanceOf[_root_.eu.stratosphere.emma.api.ParallelizedDataBag[$tpe,
      _root_.org.apache.spark.rdd.RDD[$tpe]]].repr"""
  }

  private def opCode[A](op: ir.TempSink[A])
      (implicit closure: DataFlowClosure): Tree =
    generateOpCode(op.xs)

  private def opCode[B](op: ir.Scatter[B])
      (implicit closure: DataFlowClosure): Tree = {
    val tpe = typeOf(op.tag).precise
    // add a dedicated closure variable to pass the scattered term
    val input = $"input$$spark"
    closure.localInputParams += input -> tq"_root_.scala.Seq[$tpe]"
    q"$sc.parallelize($input)"
  }

  private def opCode[B, A](op: ir.Map[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // generate fn UDF
    val mapFun = parseCheck(op.f)
    val mapUDF = ir.UDF(mapFun, mapFun.preciseType, tb)
    closure.capture(mapUDF)
    // assemble dataFlow fragment
    q"$xs.map(${mapUDF.func})"
  }

  private def opCode[B, A](op: ir.FlatMap[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // generate fn UDF
    val fmFun = parseCheck(op.f)
    val fmUDF = ir.UDF(fmFun, fmFun.preciseType, tb)
    closure.capture(fmUDF)
    q"$xs.flatMap((..${fmUDF.params}) => ${fmUDF.body}.fetch())"
  }


  private def opCode[A](op: ir.Filter[A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // generate fn UDF
    val predicate = parseCheck(op.p)
    val fUDF = ir.UDF(predicate, predicate.preciseType, tb)
    closure.capture(fUDF)
    // assemble dataflow fragment
    q"$xs.filter(${fUDF.func})"
  }

  private def opCode[C, A, B](op: ir.EquiJoin[C, A, B])
      (implicit closure: DataFlowClosure): Tree = {
    val kx = parseCheck(op.keyx)
    val kxUDF = ir.UDF(kx, kx.preciseType, tb)
    val ky = parseCheck(op.keyy)
    val kyUDF = ir.UDF(ky, ky.preciseType, tb)
    val predicate = parseCheck(op.f)
    val fUDF = ir.UDF(predicate, predicate.preciseType, tb)
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)
    val $(left, right) = $("xs$spark", "ys$spark")
    closure.capture(kxUDF, kyUDF, fUDF)
    // assemble dataflow fragment
    q"""val $left = $xs.map({ (..${kxUDF.params}) =>
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
    val xs = generateOpCode(op.xs)
    // get fold components
    val empty = parseCheck(op.empty)
    val emptyUDF = ir.UDF(empty, empty.preciseType, tb)
    val sng = parseCheck(op.sng)
    val sngUDF = ir.UDF(sng, sng.preciseType, tb)
    val union = parseCheck(op.union)
    val unionUDF = ir.UDF(union, union.preciseType, tb)
    // add closure parameters
    closure.capture(emptyUDF, sngUDF, unionUDF)
    // assemble dataFlow fragment
    q"""$xs.map(${sngUDF.func})
      .aggregate(${empty.as[Function].body})(${unionUDF.func}, ${unionUDF.func})"""
  }

  private def opCode[B, A](op: ir.FoldGroup[B, A])
      (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // get fold components
    val key = parseCheck(op.key)
    val keyUDF = ir.UDF(key, key.preciseType, tb)
    val empty = parseCheck(op.empty)
    val emptyUDF = ir.UDF(empty, empty.preciseType, tb)
    val sng = parseCheck(op.sng)
    val sngUDF = ir.UDF(sng, sng.preciseType, tb)
    val union = parseCheck(op.union)
    val unionUDF = ir.UDF(union, union.preciseType, tb)
    // add closure parameters
    closure.capture(keyUDF, emptyUDF, sngUDF, unionUDF)
    // assemble dataFlow fragment
    val aliases = for {
      (alias, origin) <- sngUDF.params zip keyUDF.params
      if alias.name != origin.name
    } yield alias.name -> Ident(origin.name)

    val $(x, y) = $("x$spark", "y$spark")

    // Note: it looks like as if reduceByKey could be changed to aggregateByKey like this:
    // .aggregateByKey(${empty.as[Function].body})(${unionUDF.func}, ${unionUDF.func})
    // but this makes LabelRankTest fail: in the `val max = mm.values.max` line,
    // `values` ends up empty somehow, which makes the max fail.
    q"""$xs.map({ (..${keyUDF.params}) =>
        (${keyUDF.body}, ${bind(sngUDF.body)(aliases: _*)})
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
    val $(key, iterator) = $("key$spark", "iterator$spark")
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // generate key UDF
    val keyFun = parseCheck(op.key)
    val keyUDF = ir.UDF(keyFun, keyFun.preciseType, tb)
    closure.capture(keyUDF)
    // assemble dataFlow fragment
    q"""$xs.groupBy(${keyUDF.func}).map({ case ($key, $iterator) =>
        _root_.eu.stratosphere.emma.api.Group($key,
          _root_.eu.stratosphere.emma.api.DataBag($iterator.toBuffer))
      })"""
  }

  private def opCode[S, K](op: ir.StatefulCreate[S, K])
      (implicit closure: DataFlowClosure): Tree = {
    val S = typeOf(op.tagS).precise
    val K = typeOf(op.tagK).precise
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    q"new _root_.eu.stratosphere.emma.runtime.spark.StatefulBackend[$S, $K]($sc, $xs)"
  }

  private def opCode[S, K](op: ir.StatefulFetch[S, K])
      (implicit closure: DataFlowClosure): Tree = {
    val S = typeOf(op.tag).precise
    val K = typeOf(op.tagK).precise
    closure.closureParams +=
      TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).precise)

    q"""${TermName(op.name)}
      .asInstanceOf[_root_.eu.stratosphere.emma.runtime.spark.StatefulBackend[$S, $K]]
      .fetchToStateLess()"""
  }

  private def opCode[S, K, U, R](op: ir.UpdateWithZero[S, K, R])
      (implicit closure: DataFlowClosure): Tree = {
    val S = typeOf(op.tag).precise
    val K = typeOf(op.tagK).precise
    val R = typeOf(op.tag).precise
    closure.closureParams +=
      TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).precise)

    val updFun = parseCheck(op.udf)
    val updUDF = ir.UDF(updFun, updFun.tpe, tb)
    closure.capture(updUDF)

    q"""${TermName(op.name)}
      .asInstanceOf[_root_.eu.stratosphere.emma.runtime.spark.StatefulBackend[$S, $K]]
      .updateWithZero[$R](${updUDF.func})"""
  }

  private def opCode[S, K, U, R](op: ir.UpdateWithOne[S, K, U, R])
      (implicit closure: DataFlowClosure): Tree = {
    val S = typeOf(op.tag).precise
    val K = typeOf(op.tagK).precise
    val U = typeOf(op.tagU).precise
    val R = typeOf(op.tag).precise
    closure.closureParams +=
      TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).dealias)

    val updates = generateOpCode(op.updates)
    val keyFun = parseCheck(op.updateKeySel)
    val keyUDF = ir.UDF(keyFun, keyFun.tpe, tb)
    val updFun = parseCheck(op.udf)
    val updUDF = ir.UDF(updFun, updFun.tpe, tb)
    closure.capture(keyUDF, updUDF)

    q"""${TermName(op.name)}
      .asInstanceOf[_root_.eu.stratosphere.emma.runtime.spark.StatefulBackend[$S, $K]]
      .updateWithOne[$U, $R]($updates, ${keyUDF.func}, ${updUDF.func})"""
  }

  private def opCode[S, K, U, R](op: ir.UpdateWithMany[S, K, U, R])
      (implicit closure: DataFlowClosure): Tree = {
    val S = typeOf(op.tag).precise
    val K = typeOf(op.tagK).precise
    val U = typeOf(op.tagU).precise
    val R = typeOf(op.tag).precise
    closure.closureParams +=
      TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).dealias)

    val updates = generateOpCode(op.updates)
    val keyFun = parseCheck(op.updateKeySel)
    val keyUDF = ir.UDF(keyFun, keyFun.tpe, tb)
    val updFun = parseCheck(op.udf)
    val updUDF = ir.UDF(updFun, updFun.tpe, tb)
    closure.capture(keyUDF, updUDF)

    q"""${TermName(op.name)}
      .asInstanceOf[_root_.eu.stratosphere.emma.runtime.spark.StatefulBackend[$S, $K]]
      .updateWithMany[$U, $R]($updates, ${keyUDF.func}, ${updUDF.func})"""
  }

  // --------------------------------------------------------------------------
  // Auxiliary structures
  // --------------------------------------------------------------------------

  private final class DataFlowClosure {
    val closureParams = mutable.Map.empty[TermName, Tree]
    val localInputParams = mutable.Map.empty[TermName, Tree]
    def capture(fs: ir.UDF*) = for (f <- fs)
      closureParams ++= f.closure map { p => p.name -> p.tpt }
  }
}
