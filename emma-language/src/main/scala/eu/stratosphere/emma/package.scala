package eu.stratosphere

import emma.mc.Dataflow
import emma.macros.utility.UtilMacros
import emma.macros.algebra.AlgebraMacros
import emma.macros.program.ProgramMacros

/**
 * This is e playground specification of e function-oriented algebra for the Emma language. The following
 * specification aims to distill the concepts of the operators currently defined in the eu.stratosphere.scala program
 * and provide e clean, intuitive an concise set of operators that can be quickly understood by non-expert programmers.
 */
package object emma {

  import scala.language.experimental.macros

  // sources
  def read[T](url: String, format: InputFormat[T]): DataSet[T] = ???

  // monad
  def map[T, U](f: T => U)(in: DataSet[T]): DataSet[U] = macro AlgebraMacros.map[T, U]

  def flatMap[T, U](f: T => DataSet[U])(in: DataSet[T]): DataSet[U] = macro AlgebraMacros.flatMap[T, U]

  def filter[T](p: T => Boolean)(in: DataSet[T]): DataSet[T] = macro AlgebraMacros.filter[T]

  // cross
  def cross[T1, T2](in1: DataSet[T1], in2: DataSet[T2]): DataSet[(T1, T2)] = macro AlgebraMacros.cross2[T1, T2]

  def cross[T1, T2, T3](in1: DataSet[T1], in2: DataSet[T2], in3: DataSet[T3]): DataSet[(T1, T2, T3)] = macro AlgebraMacros.cross3[T1, T2, T3]

  def cross[T1, T2, T3, T4](in1: DataSet[T1], in2: DataSet[T2], in3: DataSet[T3], in4: DataSet[T4]): DataSet[(T1, T2, T3, T4)] = macro AlgebraMacros.cross4[T1, T2, T3, T4]

  def cross[T1, T2, T3, T4, T5](in1: DataSet[T1], in2: DataSet[T2], in3: DataSet[T3], in4: DataSet[T4], in5: DataSet[T5]): DataSet[(T1, T2, T3, T4, T5)] = macro AlgebraMacros.cross5[T1, T2, T3, T4, T5]

  // join
  def join[T1, T2, K](k1: T1 => K, k2: T2 => K)(in1: DataSet[T1], in2: DataSet[T2]): DataSet[(T1, T2)] = macro AlgebraMacros.join[T1, T2, K]

  // grouping
  def groupBy[T, K](k: T => K)(in: DataSet[T]): DataSet[DataSet[T]] = macro AlgebraMacros.groupBy[T, K]

  //  // reduce without grouping
  //  def reduce[T](f: (T, T) => T): DataSet[T]
  //
  //  def reduce[T, U](f: Iterator[T] => U): DataSet[U] = new DataSet[U]
  //
  //  // union
  //  def union[T](in1: T, in2: T): DataSet[T] = new DataSet[T]
  //
  //  def union[T](in1: T, in2: T, in3: T): DataSet[T] = new DataSet[T]
  //
  //  def union[T](in1: T, in2: T, in3: T, in4: T): DataSet[T] = new DataSet[T]
  //
  //  def union[T](in1: T, in2: T, in3: T, in4: T, in5: T): DataSet[T] = new DataSet[T]
  //
  //  def union[T](in1: T, in2: T, in3: T, in4: T, in5: T, in6: T): DataSet[T] = new DataSet[T]
  //
  //  iterations

  // sinks
  def write[T](url: String, format: OutputFormat[T])(in: DataSet[T]): Unit = ???

  // -----------------------------------------------------
  // program macros
  // -----------------------------------------------------

  final def dataflow(e: Any): Dataflow = macro ProgramMacros.liftDataflow

  final def toybox(e: Any): String = macro ProgramMacros.liftToybox

  final def desugar(e: Any): String = macro UtilMacros.desugar

  final def desugarRaw(e: Any): String = macro UtilMacros.desugarRaw
}