package eu.stratosphere

import _root_.eu.stratosphere.emma.ir.Dataflow
import _root_.eu.stratosphere.emma.macros.utility.UtilMacros
import _root_.eu.stratosphere.emma.macros.algebra.AlgebraMacros
import _root_.eu.stratosphere.emma.macros.program.DataflowMacros

/**
 * This is e playground specification of e function-oriented algebra for the Emma language. The following
 * specification aims to distill the concepts of the operators currently defined in the eu.stratosphere.scala program
 * and provide e clean, intuitive an concise set of operators that can be quickly understood by non-expert programmers.
 */
package object emma {

  import scala.language.experimental.macros

  // sources
  def read[T](url: String, format: InputFormat[T]): DataBag[T] = ???

  // monad
  def map[T, U](f: T => U)(in: DataBag[T]): DataBag[U] = macro AlgebraMacros.map[T, U]

  def flatMap[T, U](f: T => DataBag[U])(in: DataBag[T]): DataBag[U] = macro AlgebraMacros.flatMap[T, U]

  def filter[T](p: T => Boolean)(in: DataBag[T]): DataBag[T] = macro AlgebraMacros.filter[T]

  // cross
  def cross[T1, T2](in1: DataBag[T1], in2: DataBag[T2]): DataBag[(T1, T2)] = macro AlgebraMacros.cross2[T1, T2]

  def cross[T1, T2, T3](in1: DataBag[T1], in2: DataBag[T2], in3: DataBag[T3]): DataBag[(T1, T2, T3)] = macro AlgebraMacros.cross3[T1, T2, T3]

  def cross[T1, T2, T3, T4](in1: DataBag[T1], in2: DataBag[T2], in3: DataBag[T3], in4: DataBag[T4]): DataBag[(T1, T2, T3, T4)] = macro AlgebraMacros.cross4[T1, T2, T3, T4]

  def cross[T1, T2, T3, T4, T5](in1: DataBag[T1], in2: DataBag[T2], in3: DataBag[T3], in4: DataBag[T4], in5: DataBag[T5]): DataBag[(T1, T2, T3, T4, T5)] = macro AlgebraMacros.cross5[T1, T2, T3, T4, T5]

  // join
  def join[T1, T2, K](k1: T1 => K, k2: T2 => K)(in1: DataBag[T1], in2: DataBag[T2]): DataBag[(T1, T2)] = macro AlgebraMacros.join[T1, T2, K]

  // grouping
  def groupBy[T, K](k: T => K)(in: DataBag[T]): DataBag[DataBag[T]] = macro AlgebraMacros.groupBy[T, K]

  // collection conversions
  def distinct[T](in: DataBag[T]): DataSet[T] = macro AlgebraMacros.distinct[T]

  //  // reduce without grouping
  //  def reduce[T](f: (T, T) => T): DataBag[T]
  //
  //  def reduce[T, U](f: Iterator[T] => U): DataBag[U] = new DataBag[U]
  //
  //  // union
  //  def union[T](in1: T, in2: T): DataBag[T] = new DataBag[T]
  //
  //  def union[T](in1: T, in2: T, in3: T): DataBag[T] = new DataBag[T]
  //
  //  def union[T](in1: T, in2: T, in3: T, in4: T): DataBag[T] = new DataBag[T]
  //
  //  def union[T](in1: T, in2: T, in3: T, in4: T, in5: T): DataBag[T] = new DataBag[T]
  //
  //  def union[T](in1: T, in2: T, in3: T, in4: T, in5: T, in6: T): DataBag[T] = new DataBag[T]
  //
  //  iterations

  // sinks
  def write[T](url: String, format: OutputFormat[T])(in: DataBag[T]): Unit = ???

  // -----------------------------------------------------
  // program macros
  // -----------------------------------------------------

  final def dataflow(e: Any): Dataflow = macro DataflowMacros.dataflow

  final def desugar(e: Any): String = macro UtilMacros.desugar

  final def desugarRaw(e: Any): String = macro UtilMacros.desugarRaw
}