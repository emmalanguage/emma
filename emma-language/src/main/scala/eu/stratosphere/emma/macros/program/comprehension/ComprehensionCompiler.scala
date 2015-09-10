package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.comprehension.rewrite.ComprehensionCombination
import eu.stratosphere.emma.macros.program.controlflow.ControlFlowModel

private[emma] trait ComprehensionCompiler
    extends ControlFlowModel
    with ComprehensionAnalysis
    with ComprehensionCombination {

  import universe._

  /**
   * Compiles a generic driver for a data-parallel runtime.
   *
   * @param tree The original program tree.
   * @param cfGraph The control flow graph representation of the tree.
   * @param comprehensionView A view over the comprehended terms in the tree.
   * @return A tree representing the compiled triver.
   */
  def compile(tree: Tree, cfGraph: CFGraph, comprehensionView: ComprehensionView): Tree =
    new Compiler(cfGraph, comprehensionView).transform(tree).unTypeChecked

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

      // extract the comprehension closure
      val c = t.comprehension.freeTerms

      // get the TermName associated with this comprehended term
      val name = t.definition.collect({
        case ValDef(_, n, _, _) => n.encodedName
      }).getOrElse(t.id.encodedName)

      val executeMethod = root match {
        case combinator.Write(_, _, xs) => TermName("executeWrite")
        case combinator.Fold(_, _, _, _, _) => TermName("executeFold")
        case _ => TermName("executeTempSink")
      }

      q"""
      {
        // required imports
        import _root_.scala.reflect.runtime.universe._
        import _root_.eu.stratosphere.emma.ir

        val __root = ${serialize(root)}

        // execute the plan and return a reference to the result
        engine.$executeMethod(__root, ${Literal(Constant(name.toString))}, ..${c.map(s => q"${s.name}")})
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
        case combinator.Map(f, xs) =>
          q"ir.Map[${e.elementType}, ${xs.elementType}](${serialize(f)}, ${serialize(xs)})"
        case combinator.FlatMap(f, xs) =>
          q"ir.FlatMap[${e.elementType}, ${xs.elementType}](${serialize(f)}, ${serialize(xs)})"
        case combinator.Filter(p, xs) =>
          q"ir.Filter[${e.elementType}](${serialize(p)}, ${serialize(xs)})"
        case combinator.EquiJoin(keyx, keyy, xs, ys) =>
          val xsTpe = xs.elementType
          val ysTpe = ys.elementType
          q"ir.EquiJoin[${e.elementType}, $xsTpe, $ysTpe](${serialize(keyx)}, ${serialize(keyy)}, ${serialize(c.typecheck(q"(x: $xsTpe, y: $ysTpe) => (x, y)"))}, ${serialize(xs)}, ${serialize(ys)})"
        case combinator.Cross(xs, ys) =>
          val xsTpe = xs.elementType
          val ysTpe = ys.elementType
          q"ir.Cross[${e.elementType}, $xsTpe, $ysTpe](${serialize(c.typecheck(q"(x: $xsTpe, y: $ysTpe) => (x, y)"))}, ${serialize(xs)}, ${serialize(ys)})"
        case combinator.Group(key, xs) =>
          q"ir.Group[${e.elementType}, ${xs.elementType}](${serialize(key)}, ${serialize(xs)})"
        case combinator.Fold(empty, sng, union, xs, _) =>
          q"ir.Fold[${e.tpe}, ${xs.elementType}](${serialize(empty)}, ${serialize(sng)}, ${serialize(union)}, ${serialize(xs)})"
        case combinator.FoldGroup(key, empty, sng, union, xs) =>
          q"ir.FoldGroup[${e.elementType}, ${xs.elementType}](${serialize(key)}, ${serialize(empty)}, ${serialize(sng)}, ${serialize(union)}, ${serialize(xs)})"
        case combinator.Distinct(xs) =>
          q"ir.Distinct(${serialize(xs)})"
        case combinator.Union(xs, ys) =>
          q"ir.Union(${serialize(xs)}, ${serialize(ys)})"
        case combinator.Diff(xs, ys) =>
          q"ir.Diff(${serialize(xs)}, ${serialize(ys)})"
        case ScalaExpr(_, Apply(fn, List(values))) if api.apply.alternatives.contains(fn.symbol) =>
          q"ir.Scatter(${transform(values)})"
        case _ =>
          EmptyTree
        //throw new RuntimeException("Unsupported serialization of non-combinator expression:\n" + prettyprint(e) + "\n")
      }
    }

    /**
     * Emits a reified version of the given term tree.
     *
     * @param tree The tree to be serialized.
     * @return
     */
    private def serialize(tree: Tree): String = showCode(q"""(..${for (sym <- tree.freeTerms)
      yield ValDef(Modifiers(Flag.PARAM), sym.name, tq"${sym.info}", EmptyTree)}) => {
        ${tree.unTypeChecked}
    }""".typeChecked, printRootPkg = true)
  }
}
