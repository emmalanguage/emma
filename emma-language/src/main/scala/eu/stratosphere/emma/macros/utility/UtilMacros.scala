package eu.stratosphere.emma.macros.utility

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class UtilMacros(val c: blackbox.Context) {

  import c.universe._

  def desugar(e: c.Expr[Any]) = {
    val s = show(e.tree)
    c.Expr(
      Literal(Constant(s))
    )
  }

  def desugarRaw(e: c.Expr[Any]) = {
    val s = showRaw(e.tree)
    c.Expr(
      Literal(Constant(s))
    )
  }

  def analyze[T: c.WeakTypeTag](e: c.Expr[T]): c.Expr[T] = {
    val ownerClassSym = getOwnerClass(c.internal.enclosingOwner)

    val normalizer = new EnclosingClassSelectNormalizer(ownerClassSym)
    val normalizedTree = normalizer.transform(e.tree)

    val newCode = q"""
        ..${for ((sym, select) <- normalizer.selects.toSeq) yield {
          ValDef(Modifiers(), TermName("__this$"+sym.name), TypeTree(), select)
        }}

        val result = ${normalizedTree}

        ..${for ((sym, select) <- normalizer.selects.toSeq;
                 if (select.symbol.asTerm.isGetter && !select.symbol.asTerm.isStable)) yield {
            q"${select} = ${TermName(s"__this$$${sym.name}")}"
        }}

        result
      """

    /*

     */

    val result = c.Expr(newCode)
    result
  }

  private def getOwnerClass(c: Symbol): Symbol = {
    if (c.isClass) c else getOwnerClass(c.owner)
  }

  private class EnclosingClassSelectNormalizer(val classSymbol: Symbol) extends Transformer {
    val selects = collection.mutable.Map.empty[Symbol, Select]

    override def transform(t: Tree) = t match {
      case select@Select(encl@This(_), n)
          if encl.symbol == classSymbol && select.symbol.isTerm && (select.symbol.asTerm.isStable || select.symbol.asTerm.isGetter) =>
        selects += select.symbol -> select
        q"${TermName(s"__this$$$n")}"
      case _ => super.transform(t)
    }
  }
}
