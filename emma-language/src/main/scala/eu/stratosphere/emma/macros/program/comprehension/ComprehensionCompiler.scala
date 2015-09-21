package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.comprehension.rewrite.ComprehensionCombination
import eu.stratosphere.emma.macros.program.controlflow.ControlFlowModel

private[emma] trait ComprehensionCompiler
    extends ControlFlowModel
    with ComprehensionAnalysis
    with ComprehensionCombination {

  import universe._

  /**
   * Compile a generic driver for a data-parallel runtime.
   *
   * @param tree The original program [[Tree]]
   * @param cfGraph The control flow graph representation of the [[Tree]]
   * @param compView A view over the comprehended terms in the [[Tree]]
   * @return A [[Tree]] representing the compiled driver
   */
  def compile(tree: Tree, cfGraph: CFGraph, compView: ComprehensionView): Tree =
    new Compiler(cfGraph, compView).transform(tree).unTypeChecked

  private class Compiler(cfGraph: CFGraph, compView: ComprehensionView) extends Transformer {

    override def transform(tree: Tree): Tree = compView getByTerm tree match {
      case Some(term) => expandComprehension(tree, cfGraph, compView)(term)
      case None       => tree match {
        case Apply(fn, values :: Nil) if api.apply.alternatives contains fn.symbol =>
          expandScatter(transform(values))

        case _ => super.transform(tree)
      }
    }

    /**
     * Expand a local collection constructor as a scatter call to the underlying engine.
     *
     * @param values A `Seq[T]` typed term that provides the values for the local constructor call
     * @return The [[Tree]] of a scatter call to the underlying engine
     */
    private def expandScatter(values: Tree) = q"engine.scatter($values)"

    /**
     * Expand a comprehended term in the compiled driver.
     *
     * @param tree The original program [[Tree]]
     * @param cfGraph The control flow graph representation of the [[Tree]]
     * @param compView A view over the comprehended terms in the [[Tree]]
     * @param term The comprehended term to be expanded
     * @return A [[Tree]] representing the expanded comprehension
     */
    private def expandComprehension(tree: Tree, cfGraph: CFGraph, compView: ComprehensionView)
        (term: ComprehendedTerm) = {

      // Apply combinators to get a purely-functional, logical plan
      val root = combine(term.comprehension).expr

      // Extract the comprehension closure
      val closure = term.comprehension.freeTerms

      // Get the TermName associated with this comprehended term
      val name = term.definition collect {
        case vd: ValDef => vd.name.encodedName
      } getOrElse term.id.encodedName

      val execute = root match {
        case _: combinator.Write => TermName("executeWrite")
        case _: combinator.Fold  => TermName("executeFold")
        case _                   => TermName("executeTempSink")
      }

      q"""{
        import _root_.scala.reflect.runtime.universe._
        import _root_.eu.stratosphere.emma.ir
        val __root = ${serialize(root)}
        engine.$execute(__root, ${name.toString}, ..${
          for (s <- closure) yield if (s.isMethod) q"${s.name} _" else q"${s.name}"
        })
      }"""
    }

    /**
     * Serializes a macro-level IR [[Tree]] as code constructing an equivalent runtime-level IR
     * [[Tree]].
     *
     * @param expr The [[Expression]] in IR to be serialized
     * @return A [[String]] representation of the [[Expression]]
     */
    private def serialize(expr: Expression): Tree = expr match {
      case combinator.Read(loc, fmt) =>
        q"ir.Read($loc, $fmt)"

      case combinator.Write(loc, fmt, xs) =>
        q"ir.Write($loc, $fmt, ${serialize(xs)})"

      case combinator.TempSource(id) =>
        q"ir.TempSource($id)"

      case combinator.TempSink(name, xs) =>
        q"ir.TempSink(${name.toString}, ${serialize(xs)})"

      case combinator.Map(f, xs) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val fStr  = serialize(f)
        val xsStr = serialize(xs)
        q"ir.Map[$elTpe, $xsTpe]($fStr, $xsStr)"

      case combinator.FlatMap(f, xs) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val fStr  = serialize(f)
        val xsStr = serialize(xs)
        q"ir.FlatMap[$elTpe, $xsTpe]($fStr, $xsStr)"

      case combinator.Filter(p, xs) =>
        val elTpe = expr.elementType
        val pStr  = serialize(p)
        val xsStr = serialize(xs)
        q"ir.Filter[$elTpe]($pStr, $xsStr)"

      case combinator.EquiJoin(kx, ky, xs, ys) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val ysTpe = ys.elementType
        val kxStr = serialize(kx)
        val kyStr = serialize(ky)
        val xsStr = serialize(xs)
        val ysStr = serialize(ys)
        val join  = serialize(q"(x: $xsTpe, y: $ysTpe) => (x, y)".typeChecked)
        q"ir.EquiJoin[$elTpe, $xsTpe, $ysTpe]($kxStr, $kyStr, $join, $xsStr, $ysStr)"

      case combinator.Cross(xs, ys) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val ysTpe = ys.elementType
        val xsStr = serialize(xs)
        val ysStr = serialize(ys)
        val join  = serialize(q"(x: $xsTpe, y: $ysTpe) => (x, y)".typeChecked)
        q"ir.Cross[$elTpe, $xsTpe, $ysTpe]($join, $xsStr, $ysStr)"

      case combinator.Group(key, xs) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val kStr  = serialize(key)
        val xsStr = serialize(xs)
        q"ir.Group[$elTpe, $xsTpe]($kStr, $xsStr)"

      case combinator.Fold(empty, sng, union, xs, _) =>
        val exprTpe  = expr.tpe
        val xsTpe    = xs.elementType
        val emptyStr = serialize(empty)
        val sngStr   = serialize(sng)
        val unionStr = serialize(union)
        val xsStr    = serialize(xs)
        q"ir.Fold[$exprTpe, $xsTpe]($emptyStr, $sngStr, $unionStr, $xsStr)"

      case combinator.FoldGroup(key, empty, sng, union, xs) =>
        val elTpe    = expr.elementType
        val xsTpe    = xs.elementType
        val keyStr   = serialize(key)
        val emptyStr = serialize(empty)
        val sngStr   = serialize(sng)
        val unionStr = serialize(union)
        val xsStr    = serialize(xs)
        q"ir.FoldGroup[$elTpe, $xsTpe]($keyStr, $emptyStr, $sngStr, $unionStr, $xsStr)"

      case combinator.Distinct(xs) =>
        q"ir.Distinct(${serialize(xs)})"

      case combinator.Union(xs, ys) =>
        q"ir.Union(${serialize(xs)}, ${serialize(ys)})"

      case combinator.Diff(xs, ys) =>
        q"ir.Diff(${serialize(xs)}, ${serialize(ys)})"

      case ScalaExpr(Apply(fn, values :: Nil))
        if api.apply.alternatives contains fn.symbol =>
          q"ir.Scatter(${transform(values)})"

      case e => EmptyTree
      //throw new RuntimeException(
      //  s"Unsupported serialization of non-combinator expression:\n${prettyPrint(e)}\n")
    }

    /**
     * Emits a reified version of the given term [[Tree]].
     *
     * @param tree The [[Tree]] to be serialized
     * @return A [[String]] representation of the [[Tree]]
     */
    private def serialize(tree: Tree): String = {
      val args = tree.freeTerms.toList
        .sortBy { _.fullName }
        .map { sym =>
          sym.name ->
            (sym.info match {
              // The following line converts a method type to a function type (eg. from "(x: Int)Int" to "Int => Int").
              // This is necessary, because this will go into a parameter list, where we can't have method types.
              case _: MethodType => q"$sym _".trueType
              case _             => sym.info
            })
        }

      val fun = mk.anonFun(args, tree)
      showCode(fun, printRootPkg = true)
    }
  }
}
