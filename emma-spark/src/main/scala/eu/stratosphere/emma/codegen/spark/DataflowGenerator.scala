package eu.stratosphere.emma.codegen.spark

import java.net.URI
import java.util.UUID

import eu.stratosphere.emma.api.{CSVOutputFormat, CSVInputFormat}
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir
import eu.stratosphere.emma.util.Counter

import scala.collection.mutable
import scala.reflect.runtime.universe._


class DataflowGenerator(val dataflowCompiler: DataflowCompiler, val sessionID: UUID = UUID.randomUUID()) {

  import eu.stratosphere.emma.runtime.logger

  private val tempResultsPrefix = new URI(System.getProperty("emma.tmp.dir", s"file://${System.getProperty("java.io.tmpdir")}/emma/temp")).toString

  val tb = dataflowCompiler.tb

  val memo = mutable.Map[String, ModuleSymbol]()

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
          def run(sc: SparkContext, ..$params, ..$localInputs) = {
            $opCode
          }
          """
        case op: ir.Write[_] =>
          q"""
          def run(sc: SparkContext, ..$params, ..$localInputs) = {
            $opCode
          }
          """
        case op: ir.Combinator[_] =>
          q"""
          def run(sc: SparkContext, ..$params, ..$localInputs) = {
            val result = $opCode.cache()
            result.foreach(_ => Unit)
            result
          }
          """
      }
      // assemble dataflow
      val tree = q"""
      object ${TermName(dataflowName)} {

        import org.apache.spark.SparkContext
        import org.apache.spark.SparkContext._
        import eu.stratosphere.emma.api.DataBag

        ..${closure.udfs.result().toSeq}

        $runMethod
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
    val path: String = op.location

    val inFormatTree = op.format match {
      case ifmt: CSVInputFormat[tp] =>
        if (!(tpe <:< weakTypeOf[Product])) {
          throw new RuntimeException(s"Cannot create Flink CsvInputFormat for non-product type ${typeOf(op.tag)}")
        }

        val cname = closure.nextTermName("converter")
        closure.udfs += q"""
        val $cname = eu.stratosphere.emma.api.`package`.materializeCSVConvertors[$tpe]
        """

        q"""
        sc.textFile($path).map { line =>
          $cname.fromCSV(line.split(${ifmt.separator}))
        }
        """
      case _ =>
        throw new RuntimeException(s"Unsupported InputFormat of type '${op.format.getClass}'")
    }

    inFormatTree
  }

  private def opCode[IT](op: ir.Write[IT])(implicit closure: DataflowClosure): Tree = {
    val tpe = typeOf(op.xs.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    val outFormatTree: Tree = op.format match {
      case ofmt: CSVOutputFormat[_] =>
        val seperator: String = ofmt.separator.toString
        if (tpe <:< weakTypeOf[Product]) {
          q"""
          $xs.map { e =>
            (0 until e.productArity).map(x => e.productElement(x)).mkString($seperator)
          }
          """
        } else {
          throw new RuntimeException(s"Cannot create Spark CsvOutputFormat for non-product type ${typeOf(op.tag)}")
        }
      case _ =>
        throw new RuntimeException(s"Unsupported OutputFormat of type '${op.format.getClass}'")
    }

    // assemble dataflow fragment
    q"""
    $outFormatTree.saveAsTextFile(${op.location})
    """
  }

  private def opCode[OT](op: ir.TempSource[OT])(implicit closure: DataflowClosure): Tree = {
    val tpe = typeOf(op.tag).dealias

    // add a dedicated closure variable to pass the input param
    val param = TermName(op.ref.name)
    closure.closureParams += param -> tq"eu.stratosphere.emma.runtime.Spark.DataBagRef[$tpe]"

    q"$param.rdd"
  }

  private def opCode[IT](op: ir.TempSink[IT])(implicit closure: DataflowClosure): Tree = {
    // forward input
    generateOpCode(op.xs)
  }

  private def opCode[OT](op: ir.Scatter[OT])(implicit closure: DataflowClosure): Tree = {
    val tpe = typeOf(op.tag).dealias

    // add a dedicated closure variable to pass the scattered term
    val inputParam = closure.nextTermName()
    closure.localInputParams += inputParam -> tq"Seq[$tpe]"

    q"""
    sc.parallelize($inputParam)
    """
  }

  private def opCode[OT, IT](op: ir.Map[OT, IT])(implicit closure: DataflowClosure): Tree = {
    val tpe = typeOf(op.tag)

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // generate fn UDF
    val fnUDF = ir.UDF(op.f, tb)
    closure.closureParams ++= (for (p <- fnUDF.closure) yield p.name -> p.tpt)

    // assemble dataflow fragment
    q"""
    $xs.map(..${fnUDF.params} => ${fnUDF.body})
    """
  }

  private def opCode[OT, IT](op: ir.FlatMap[OT, IT])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val srcTpe = typeOf(op.xs.tag).dealias
    val tgtTpe = typeOf(op.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // generate fn UDF
    val fnUDF = ir.UDF(op.f, tb)
    closure.closureParams ++= (for (p <- fnUDF.closure) yield p.name -> p.tpt)

    q"""
    $xs.flatMap(..${fnUDF.params} => (${fnUDF.body}).fetch())
    """
  }


  private def opCode[IT](op: ir.Filter[IT])(implicit closure: DataflowClosure): Tree = {
    val tpe = typeOf(op.tag)

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // generate fn UDF
    val fnUDF = ir.UDF(op.p, tb)
    closure.closureParams ++= (for (p <- fnUDF.closure) yield p.name -> p.tpt)

    // assemble dataflow fragment
    q"""
    $xs.filter(..${fnUDF.params} => ${fnUDF.body})
    """
  }

  private def opCode[OT, IT1, IT2](op: ir.Cross[OT, IT1, IT2])(implicit closure: DataflowClosure): Tree = {
    val xsTpe = typeOf(op.xs.tag).dealias
    val ysTpe = typeOf(op.ys.tag).dealias

    val fTpe = ir.resultType(op.f.staticType.dealias)

    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)

    // assemble dataflow fragment
    q"""
    $xs.cartesian($ys)
    """
  }

  private def opCode[OT, IT1, IT2](op: ir.EquiJoin[OT, IT1, IT2])(implicit closure: DataflowClosure): Tree = {
    // infer types and generate type information
    val xsTpe = typeOf(op.xs.tag).dealias
    val ysTpe = typeOf(op.ys.tag).dealias

    val keyxTpe = ir.resultType(op.keyx.staticType.dealias)

    val keyyTpe = ir.resultType(op.keyy.staticType.dealias)

    val fTpe = ir.resultType(op.f.staticType.dealias)

    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)

    // generate keyx UDF
    val keyxUDF = ir.UDF(op.keyx, tb)
    closure.closureParams ++= (for (p <- keyxUDF.closure) yield p.name -> p.tpt)

    // generate keyy UDF
    val keyyUDF = ir.UDF(op.keyy, tb)
    closure.closureParams ++= (for (p <- keyyUDF.closure) yield p.name -> p.tpt)

    // generate join UDF
    val fUDF = ir.UDF(op.f, tb)
    closure.closureParams ++= (for (p <- fUDF.closure) yield p.name -> p.tpt)

    // assemble dataflow fragment
    q"""
    val __xs = $xs.map(..${keyxUDF.params} => (${keyxUDF.body}, ${keyxUDF.params.head.name}))
    val __ys = $ys.map(..${keyyUDF.params} => (${keyyUDF.body}, ${keyyUDF.params.head.name}))

    __xs.join(__ys).map{ case (k, v) => v}
    """
  }

  private def opCode[OT, IT](op: ir.Fold[OT, IT])(implicit closure: DataflowClosure): Tree = {
    val srcTpe = typeOf(op.xs.tag).dealias
    val tgtTpe = typeOf(op.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // get fold components
    val sng = ir.UDF(op.sng, tb)
    val union = ir.UDF(op.union, tb)

    // assemble dataflow fragment
    q"""
    $xs.map(..${sng.params} => ${sng.body})
       .reduce(..${union.params} => ${union.body})
    """
  }

  private def opCode[OT, IT](op: ir.FoldGroup[OT, IT])(implicit closure: DataflowClosure): Tree = {
    val srcTpe = typeOf(op.xs.tag).dealias
    val tgtTpe = typeOf(op.tag).dealias

    // assemble input fragment
    val xs = generateOpCode(op.xs)

    // get fold components
    val key = ir.UDF(op.key, tb)
    val sng = ir.UDF(op.sng, tb)
    val union = ir.UDF(op.union, tb)

    // assemble dataflow fragment
    closure.closureParams ++= (for (p <- key.closure) yield p.name -> p.tpt)
    closure.closureParams ++= (for (p <- sng.closure) yield p.name -> p.tpt)
    closure.closureParams ++= (for (p <- union.closure) yield p.name -> p.tpt)

    q"""
    $xs.map(..${key.params} => (${key.body}, ${sng.body}))
       .reduceByKey(..${union.params} => ${union.body})
       .map(x => eu.stratosphere.emma.api.Group(x._1, x._2))
    """
  }

  private def opCode[T](op: ir.Distinct[T])(implicit closure: DataflowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)

    q"$xs.distinct()"
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

    def nextTermName(prefix: String = "scatter") = TermName(f"$prefix${scatterCounter.advance.get}%05d")
  }

  def substitute(map: (TermName, Tree)*) = {
    val m = Map(map: _*)
    new SymbolSubstituter(Map(map: _*))
  }

  class SymbolSubstituter(map: Map[TermName, Tree]) extends Transformer with (Tree => Tree) {
    override def apply(tree: Tree): Tree = transform(tree)

    override def transform(tree: Tree) = tree match {
      case Ident(name@TermName(_)) if map.contains(name) => map(name)
      case _ => super.transform(tree)
    }
  }

}
