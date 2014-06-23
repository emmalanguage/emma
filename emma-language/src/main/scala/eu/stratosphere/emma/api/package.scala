package eu.stratosphere.emma

import eu.stratosphere.emma.api.model.Identity
import eu.stratosphere.emma.ir.Workflow
import eu.stratosphere.emma.macros.utility.UtilMacros
import eu.stratosphere.emma.macros.program.WorkflowMacros

/**
 * This is e playground specification of e function-oriented algebra for the Emma language.
 */
package object api {

  import scala.language.experimental.macros

  // -----------------------------------------------------
  // language primitives
  // -----------------------------------------------------

  /**
   * Reads a DataBag from the given `url` location using the provided `format`.
   *
   * @param url The location of the constructed DataSet.
   * @param format The format used to read DataSet elements.
   * @tparam A The type of the DataSet elements.
   * @return A DataBag read from the given `url`.
   */
  def read[A](url: String, format: InputFormat[A]): DataBag[A] = ???

  /**
   * Writes a DataBag `in` to a `url` using the provided `format`.
   *
   * @param url The location where the DataSet should be written.
   * @param format The format used to write DataSet elements.
   * @param in The set to be written.
   * @tparam A The type of the DataSet elements.
   * @return Unit
   */
  def write[A](url: String, format: OutputFormat[A])(in: Collection[A]): Unit = ???

  /**
   * Converts an immutable DataBag into a stateful version which permits in-place pointwise updates.
   *
   * @param bag The bag to be used to initialize the state.
   * @tparam A The type of the state elements. Must be a subtype of Identity[K]
   * @tparam K The key to be used to index the elements.
   * @return A Stateful.Bag instance initialized with the elements of the given `bag`.
   */
  def stateful[A <: Identity[K], K](bag: DataBag[A]): Stateful.Bag[A, K] = new Stateful.Bag[A, K](bag)

  // -----------------------------------------------------
  // program macros
  // -----------------------------------------------------

  final def workflow(e: Any): Workflow = macro WorkflowMacros.workflow

  final def desugar(e: Any): String = macro UtilMacros.desugar

  final def desugarRaw(e: Any): String = macro UtilMacros.desugarRaw

  // -----------------------------------------------------
  // limits
  // -----------------------------------------------------

  trait Limits[T] {
    val min: T
    val max: T
  }

  implicit object ByteLimits extends Limits[Byte] {
    val min = Byte.MinValue
    val max = Byte.MaxValue
  }

  implicit object IntLimits extends Limits[Int] {
    val min = Int.MinValue
    val max = Int.MaxValue
  }

  implicit object LongLimits extends Limits[Long] {
    val min = Long.MinValue
    val max = Long.MaxValue
  }

  implicit object CharLimits extends Limits[Char] {
    val min = Char.MinValue
    val max = Char.MaxValue
  }

  implicit object FloatLimits extends Limits[Float] {
    val min = Float.MinValue
    val max = Float.MaxValue
  }

  implicit object DoubleLimits extends Limits[Double] {
    val min = Double.MinValue
    val max = Double.MaxValue
  }
}