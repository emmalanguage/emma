package eu.stratosphere.emma.macros.program.comprehension

import eu.stratosphere.emma.macros.program.comprehension.rewrite.ComprehensionCombination
import eu.stratosphere.emma.macros.program.controlflow.ControlFlowModel

private[emma] trait ComprehensionCompiler
    extends ControlFlowModel
    with ComprehensionAnalysis
    with ComprehensionCombination {

  import universe._
  import syntax._

  val ROOT = q"_root_"
  val SCALA = q"$ROOT.scala"
  val EMMA = q"$ROOT.eu.stratosphere.emma"
  val IR = q"$EMMA.ir"
  val CONTEXT = q"$EMMA.runtime.Context"

  /**
   * Compile a generic driver for a data-parallel runtime.
   *
   * @param tree The original program [[Tree]]
   * @param cfGraph The control flow graph representation of the [[Tree]]
   * @param compView A view over the comprehended terms in the [[Tree]]
   * @return A [[Tree]] representing the compiled driver
   */
  def compile(tree: Tree, cfGraph: CFGraph, compView: ComprehensionView): Tree =
    new Compiler(cfGraph, compView).compile(tree).unTypeChecked

  private class Compiler(cfGraph: CFGraph, compView: ComprehensionView) {

    def compile(tree: Tree): Tree = transform(tree) {
      case t if compView.getByTerm(t).isDefined =>
        expandComprehension(t, cfGraph, compView)(compView.getByTerm(t).get)
    }

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
      val name = term.id.encodedName
      // Get the source code positions
      val positions = if (term.pos == NoPosition) Set.empty[(Int, Int)]
        else Set(term.pos.start - 1 -> term.pos.end)

      val execute = root match {
        case _: combinator.Write          => TermName("executeWrite")
        case _: combinator.Fold           => TermName("executeFold")
        case _: combinator.StatefulCreate => TermName("executeStatefulCreate")
        case _                            => TermName("executeTempSink")
      }

      q"""{
        import $SCALA.reflect.runtime.universe._
        engine.$execute(${serialize(root)}, ${name.toString}, $CONTEXT($positions), ..${
          for (sym <- closure) yield if (sym.isMethod) q"${sym.name} _" else q"${sym.name}"
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
        q"$IR.Read($loc, $fmt)"

      case combinator.Write(loc, fmt, xs) =>
        q"$IR.Write($loc, $fmt, ${serialize(xs)})"

      case combinator.TempSource(id) =>
        q"$IR.TempSource($id)"

      case combinator.TempSink(name, xs) =>
        q"$IR.TempSink(${name.toString}, ${serialize(xs)})"

      case combinator.Map(f, xs) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val fStr  = serialize(f)
        val xsStr = serialize(xs)
        q"$IR.Map[$elTpe, $xsTpe]($fStr, $xsStr)"

      case combinator.FlatMap(f, xs) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val fStr  = serialize(f)
        val xsStr = serialize(xs)
        q"$IR.FlatMap[$elTpe, $xsTpe]($fStr, $xsStr)"

      case combinator.Filter(p, xs) =>
        val elTpe = expr.elementType
        val pStr  = serialize(p)
        val xsStr = serialize(xs)
        q"$IR.Filter[$elTpe]($pStr, $xsStr)"

      case combinator.EquiJoin(kx, ky, xs, ys) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val ysTpe = ys.elementType
        val kxStr = serialize(kx)
        val kyStr = serialize(ky)
        val xsStr = serialize(xs)
        val ysStr = serialize(ys)
        val join  = serialize(q"(x: $xsTpe, y: $ysTpe) => (x, y)".typeChecked)
        q"$IR.EquiJoin[$elTpe, $xsTpe, $ysTpe]($kxStr, $kyStr, $join, $xsStr, $ysStr)"

      case combinator.Cross(xs, ys) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val ysTpe = ys.elementType
        val xsStr = serialize(xs)
        val ysStr = serialize(ys)
        val join  = serialize(q"(x: $xsTpe, y: $ysTpe) => (x, y)".typeChecked)
        q"$IR.Cross[$elTpe, $xsTpe, $ysTpe]($join, $xsStr, $ysStr)"

      case combinator.Group(key, xs) =>
        val elTpe = expr.elementType
        val xsTpe = xs.elementType
        val kStr  = serialize(key)
        val xsStr = serialize(xs)
        q"$IR.Group[$elTpe, $xsTpe]($kStr, $xsStr)"

      case combinator.Fold(empty, sng, union, xs, _) =>
        val exprTpe  = expr.tpe
        val xsTpe    = xs.elementType
        val emptyStr = serialize(empty)
        val sngStr   = serialize(sng)
        val unionStr = serialize(union)
        val xsStr    = serialize(xs)
        q"$IR.Fold[$exprTpe, $xsTpe]($emptyStr, $sngStr, $unionStr, $xsStr)"

      case combinator.FoldGroup(key, empty, sng, union, xs) =>
        val elTpe    = expr.elementType
        val xsTpe    = xs.elementType
        val keyStr   = serialize(key)
        val emptyStr = serialize(empty)
        val sngStr   = serialize(sng)
        val unionStr = serialize(union)
        val xsStr    = serialize(xs)
        q"$IR.FoldGroup[$elTpe, $xsTpe]($keyStr, $emptyStr, $sngStr, $unionStr, $xsStr)"

      case combinator.Distinct(xs) =>
        q"$IR.Distinct(${serialize(xs)})"

      case combinator.Union(xs, ys) =>
        q"$IR.Union(${serialize(xs)}, ${serialize(ys)})"

      case combinator.Diff(xs, ys) =>
        q"$IR.Diff(${serialize(xs)}, ${serialize(ys)})"

      case ScalaExpr(Apply(fn, values :: Nil)) // one argument
        if api.apply.alternatives contains fn.symbol =>
          q"$IR.Scatter(${compile(values)})"

      case ScalaExpr(Apply(fn, Nil)) // no argument
        if api.apply.alternatives contains fn.symbol =>
          q"$IR.Scatter(Seq.empty)"

      case combinator.StatefulCreate(xs, stateType, keyType) =>
        val xsStr = serialize(xs)
        q"$IR.StatefulCreate[$stateType, $keyType]($xsStr)"

      case combinator.StatefulFetch(stateful) =>
        val statefulNameStr = stateful.name.toString
        q"$IR.StatefulFetch($statefulNameStr, $stateful)"

      case combinator.UpdateWithZero(stateful, udf) =>
        val S = stateful.preciseType.typeArgs(0)
        val K = stateful.preciseType.typeArgs(1)
        val R = expr.elementType
        val statefulName = stateful.name.toString
        val udfStr = serialize(udf)
        q"$IR.UpdateWithZero[$S, $K, $R]($statefulName, $stateful, $udfStr)"

      case combinator.UpdateWithOne(stateful, updates, key, udf) =>
        val S = stateful.preciseType.typeArgs(0)
        val K = stateful.preciseType.typeArgs(1)
        val U = updates.elementType
        val R = expr.elementType
        val statefulName = stateful.name.toString
        val updStr = serialize(updates)
        val keyStr = serialize(key)
        val udfStr = serialize(udf)
        q"$IR.UpdateWithOne[$S, $K, $U, $R]($statefulName, $stateful, $updStr, $keyStr, $udfStr)"

      case combinator.UpdateWithMany(stateful, updates, key, udf) =>
        val S = stateful.preciseType.typeArgs(0)
        val K = stateful.preciseType.typeArgs(1)
        val U = updates.elementType
        val R = expr.elementType
        val statefulName = stateful.name.toString
        val updStr = serialize(updates)
        val keyStr = serialize(key)
        val udfStr = serialize(udf)
        q"$IR.UpdateWithMany[$S, $K, $U, $R]($statefulName, $stateful, $updStr, $keyStr, $udfStr)"

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
      val args = tree.closure.toList
        .sortBy { _.name.toString }
        .map { term =>
          term.withType(term.info match {
            // The following line converts a method type to a function type (eg. from "(x: Int)Int"
            // to "Int => Int"). This is necessary, because this will go into a parameter list,
            // where we can't have method types.
            case _: MethodType => q"$term _".preciseType
            case _ => term.info
          }).asTerm
        }

      val fun = lambda(args: _*) { tree }
      showCode(fun, printRootPkg = true)
    }
  }
}
