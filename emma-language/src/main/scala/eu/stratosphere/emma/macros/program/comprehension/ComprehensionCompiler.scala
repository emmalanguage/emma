package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.ContextHolder
import eu.stratosphere.emma.macros.program.comprehension.rewrite.ComprehensionCombination
import eu.stratosphere.emma.macros.program.controlflow.ControlFlowModel
import eu.stratosphere.emma.macros.program.util.ProgramUtils

import scala.reflect.macros._

private[emma] trait ComprehensionCompiler[C <: blackbox.Context]
  extends ContextHolder[C]
  with ControlFlowModel[C]
  with ComprehensionAnalysis[C]
  with ComprehensionModel[C]
  with ComprehensionCombination[C]
  with ProgramUtils[C] {

  import c.universe._

  /**
   * Compiles a generic driver for a data-parallel runtime.
   *
   * @param tree The original program tree.
   * @param cfGraph The control flow graph representation of the tree.
   * @param comprehensionView A view over the comprehended terms in the tree.
   * @return A tree representing the compiled triver.
   */
  def compile(tree: Tree, cfGraph: CFGraph, comprehensionView: ComprehensionView): Tree = {
    val compiled = new Compiler(cfGraph, comprehensionView).transform(tree)
    c.untypecheck(compiled)
  }

  private class Compiler(cfGraph: CFGraph, comprehensionView: ComprehensionView) extends Transformer {

    override def transform(tree: Tree): Tree = comprehensionView.getByTerm(tree) match {
      case Some(t) =>
        expandComprehensionTerm(tree, cfGraph, comprehensionView)(t)
      case _ => tree match {
        case Apply(fn, List(values)) if api.apply.alternatives.contains(fn.symbol) =>
          expandScatterTerm(transform(values))
        case _ =>
          super.transform(tree)
      }
    }

    /**
     * Expands a local collection constructor as a scatter call to the underlying engine.
     *
     * @param values A Seq[T] typed term that provides the values for the local constructor call.
     */
    private def expandScatterTerm(values: Tree) = q"engine.scatter($values)"

    /**
     * Expands a comprehended term in the compiled driver.
     *
     * @param tree The original program tree.
     * @param cfGraph The control flow graph representation of the tree.
     * @param comprehensionView A view over the comprehended terms in the tree.
     * @param t The comprehended term to be expanded.
     * @return A tree representing the expanded comprehension.
     */
    private def expandComprehensionTerm(tree: Tree, cfGraph: CFGraph, comprehensionView: ComprehensionView)(t: ComprehendedTerm) = {
      // apply combinators to get a purely-functional, logical plan
      val root = combine(t.comprehension).expr

      // extract the comprehension closure, i.e. all term symbols that are defined outside of the comprehended tree
      val closure = {
        // find all term symbols referenced within the comprehended term
        val referenced = t.term.collect({
          case i@Ident(TermName(_)) if i.symbol.asTerm.isVal || i.symbol.asTerm.isVar => i.symbol.asTerm
        })

        // find all term symbols defined within the term
        val defined = t.term.collect({
          case vd@ValDef(_, _, _, _) => vd.symbol.asTerm
        }).toSet

        // find all symbols referenced in temp sources
        val tempsources = t.comprehension.expr.collect({
          case combinator.TempSource(i@Ident(TermName(_))) if i.symbol.asTerm.isVal || i.symbol.asTerm.isVar => i.symbol.asTerm
        })

        // final closure is "diff = referenced \ tempsources" (multiset difference), fo
        // followed by "diff \ defined" (set difference)
        // this protects against removal of tempsources which are also referenced from the UDF code
        val closure = (referenced diff tempsources).toSet diff defined

        // return as a list ordered lexicographically by the symbol's term names (via closure passing convention)
        closure.toList.sortBy(_.name.toString)
      }

      // get the TermName associated with this comprehended term
      val name = t.definition.collect({
        case ValDef(_, n, _, _) => n.encodedName
      }).getOrElse(t.id.encodedName)

      q"""
      {
        // required imports
        import scala.reflect.runtime.universe._
        import eu.stratosphere.emma.ir

        val __root = ${serialize(root)}

        // execute the plan and return a reference to the result
        engine.execute(__root, ${Literal(Constant(name.toString))}, ..${closure.map(_.name)})
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
          q"ir.Map[${elementType(e.tpe)}, ${elementType(xs.tpe)}](${serialize(f)}, ${serialize(xs)})"
        case combinator.FlatMap(f, xs) =>
          q"ir.FlatMap[${elementType(e.tpe)}, ${elementType(xs.tpe)}](${serialize(f)}, ${serialize(xs)})"
        case combinator.Filter(p, xs) =>
          q"ir.Filter[${elementType(e.tpe)}](${serialize(p)}, ${serialize(xs)})"
        case combinator.EquiJoin(keyx, keyy, xs, ys) =>
          q"ir.EquiJoin[${elementType(e.tpe)}, ${elementType(xs.tpe)}, ${elementType(ys.tpe)}](${serialize(keyx)}, ${serialize(keyy)}, ${serialize(xs)}, ${serialize(ys)})"
        case combinator.Cross(xs, ys) =>
          q"ir.Cross[${elementType(e.tpe)}, ${elementType(xs.tpe)}, ${elementType(ys.tpe)}](${serialize(xs)}, ${serialize(ys)})"
        case combinator.Group(key, xs) =>
          q"ir.Group[${elementType(e.tpe)}, ${elementType(xs.tpe)}](${serialize(key)}, ${serialize(xs)})"
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
        case ScalaExpr(_, Apply(fn, List(values))) if api.apply.alternatives.contains(fn.symbol) =>
          q"ir.Scatter(${transform(values)})"
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

}
