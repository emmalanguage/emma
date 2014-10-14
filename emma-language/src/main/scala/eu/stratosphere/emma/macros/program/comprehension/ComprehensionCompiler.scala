package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.comprehension.rewrite.ComprehensionCombination
import eu.stratosphere.emma.macros.program.controlflow.ControlFlowModel
import eu.stratosphere.emma.macros.program.util.ProgramUtils

import scala.reflect.macros._

private[emma] trait ComprehensionCompiler[C <: blackbox.Context]
  extends ContextHolder[C]
  with ControlFlowModel[C]
  with ComprehensionModel[C]
  with ComprehensionCombination[C]
  with ProgramUtils[C] {

  import c.universe._

  /**
   * Expands a local collection constructor as a scatter call to the underlying engine.
   *
   * @param values A Seq[T] typed term that provides the values for the local constructor call.
   */
  def expandScatterTerm(values: Tree) = q"engine.scatter($values)"

  /**
   * Expands a comprehended term in the compiled driver.
   *
   * @param tree The original program tree.
   * @param cfGraph The control flow graph representation of the tree.
   * @param comprehensionStore A store containing the comprehended terms in the tree.
   * @param t The comprehended term to be expanded.
   * @return A tree representing the expanded comprehension.
   */
  def expandComprehensionTerm(tree: Tree, cfGraph: CFGraph, comprehensionStore: ComprehensionStore)(t: ComprehendedTerm) = {
    // apply combinators to get a purely-functional, logical plan
    val root = combine(t.comprehension).expr

    q"""
    {
      // required imports
      import scala.reflect.runtime.universe._
      import eu.stratosphere.emma.ir
      import eu.stratosphere.emma.optimizer._

      // execute the plan and return a reference to the result
      engine.execute(${serialize(root)})
    }
    """

  }

  /**
   * Serializes a macro-level IR tree as code constructing an equivalent runtime-level IR tree.
   *
   * @param e The expression in IR to be serialized.
   * @return
   */
  private def serialize(e: Expression): Tree = {
    e match {
      case combinator.Read(location, format) =>
        q"ir.Read($location, $format)"
      case combinator.Write(location, format, xs) =>
        q"ir.Write($location, $format, ${serialize(xs)})"
      case combinator.TempSource(ident) =>
        q"ir.TempSource($ident)"
      case combinator.TempSink(name, xs) =>
        q"ir.TempSink(${name.toString}, ${serialize(xs)})"
      case combinator.FoldSink(name, xs) =>
        q"ir.FoldSink(${name.toString}, ${serialize(xs)})"
      case combinator.Map(f, xs) =>
        q"ir.Map(${serialize(f)}, ${serialize(xs)})"
      case combinator.FlatMap(f, xs) =>
        q"ir.FlatMap(${serialize(f)}, ${serialize(xs)})"
      case combinator.Filter(p, xs) =>
        q"ir.Filter(${serialize(p)}, ${serialize(xs)})"
      case combinator.EquiJoin(keyx, keyy, xs, ys) =>
        q"ir.EquiJoin(${serialize(keyx)}, ${serialize(keyy)}, ${serialize(xs)}, ${serialize(ys)})"
      case combinator.Cross(xs, ys) =>
        q"ir.Cross(${serialize(xs)}, ${serialize(ys)})"
      case combinator.Group(key, xs) =>
        q"ir.Group(${serialize(key)}, ${serialize(xs)})"
      case combinator.Fold(empty, sng, union, xs) =>
        q"ir.Fold(${serialize(empty)}, ${serialize(sng)}, ${serialize(union)}, ${serialize(xs)})"
      case combinator.FoldGroup(key, empty, sng, union, xs) =>
        q"ir.FoldGroup(${serialize(key)}, ${serialize(empty)}, ${serialize(sng)}, ${serialize(union)}, ${serialize(xs)})"
      case combinator.Distinct(xs) =>
        q"ir.Distinct(${serialize(xs)})"
      case combinator.Union(xs, ys) =>
        q"ir.Union(${serialize(xs)}, ${serialize(ys)})"
      case combinator.Diff(xs, ys) =>
        q"ir.Diff(${serialize(xs)}, ${serialize(ys)})"
      case _ =>
        throw new RuntimeException("Unsupported serialization of non-combinator expression:\n" + prettyprint(e) + "\n")
    }
  }

  /**
   * Emits a reified version of the given term tree.
   *
   * @param e The tree to be serialized.
   * @return
   */
  private def serialize(e: Tree): Tree = q"scala.reflect.runtime.universe.reify { $e }"

  /**
   * Fetches the element type `A` of a `DataBag[A]` type.
   *
   * @param t The reflected `DataBag[A]` type.
   * @return The `A` type.
   */
  private def elementType(t: Type): Type = t.typeArgs.head
}
