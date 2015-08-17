package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

class SparseMatrixVectorMultiplication(
      input: String, output: String, exponent: Int, rt: Engine)
    extends Algorithm(rt) {
  import eu.stratosphere.emma.examples.graphs.SparseMatrixVectorMultiplication._
  import Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns get SparseMatrixVectorMultiplication.Command.keyInput,
    ns get SparseMatrixVectorMultiplication.Command.keyOutput,
    ns get SparseMatrixVectorMultiplication.Command.keyExponent,
    rt)

  def run() = algorithm run rt

  val algorithm = emma.parallelize {
    // read vector and matrix from input
    var vector = read(s"$input/vector", new CSVInputFormat[VectorElement])
    val matrix = read(s"$input/matrix", new CSVInputFormat[MatrixElement])
    for (_ <- 1 to exponent) {
      // calculate intermediate products
      val products = for {
        mv <- matrix
        vv <- vector
        if vv.index == mv.col
      } yield mv.copy(value = vv.value * mv.value)
      // calculate the next power of the result vector
      vector = for (row <- products groupBy { _.row })
        yield VectorElement(row.key, row.values.map(_.value).sum())
    }

    val result = for (v <- vector) yield v
    write(output, new CSVOutputFormat[VectorElement]) { result }
    result
  }
}

object SparseMatrixVectorMultiplication {
  class Command extends Algorithm.Command[SparseMatrixVectorMultiplication] {
    import Command._

    override def name = "spmv"

    override def description =
      "Compute the n-th power product of a sparse matrix and a sparse vector."

    override def setup(parser: Subparser) = {
      super.setup(parser)

      parser.addArgument(keyInput)
        .`type`(classOf[String])
        .dest(keyInput)
        .metavar("INPUT")
        .help("input directory for matrix and vector")

      parser.addArgument(keyOutput)
        .`type`(classOf[String])
        .dest(keyOutput)
        .metavar("OUTPUT")
        .help("output file for result vector")

      parser.addArgument(keyExponent)
        .`type`(classOf[String])
        .dest(keyExponent)
        .metavar("EXPONENT")
        .help("number of iterations (exponent)")
    }
  }

  object Command {
    val keyInput    = "input"
    val keyOutput   = "output"
    val keyExponent = "exponent"
  }

  object Schema {
    type Index = Long
    type Value = Double

    case class VectorElement(@id index: Index, value: Value)
        extends Identity[Index] { def identity = index }

    case class MatrixElement(@id row: Index, @id col: Index, value: Value)
        extends Identity[(Index, Index)] { def identity = (row, col) }
  }
}
