package eu.stratosphere.emma.macros.program.controlflow

import eu.stratosphere.emma.macros.program.ContextHolder

import scala.reflect.macros.blackbox

private[emma] trait MacroHelper[C <: blackbox.Context] extends ContextHolder[C] {

  import c.universe._

  /**
   * Identifies all variables that are parameters from enclosing objects and assigns them to local variables at the beginning of the code.
   * @param root the tree to be analyzed
   * @tparam T
   * @return altered code with local variables
   */
  def convertEnclosingParametersToLocal[T: c.WeakTypeTag](root: Tree): Tree = {
    val ownerClassSym = getOwnerClass(c.internal.enclosingOwner)

    val normalizer = new EnclosingClassSelectNormalizer(ownerClassSym)
    val normalizedTree = normalizer.transform(root)

    val newCode = q"""
      ..${
      for ((sym, select) <- normalizer.selects.toSeq) yield {
        ValDef(Modifiers(), TermName("__this$" + sym.name), TypeTree(), select)
      }
    }
      ..${normalizedTree}
    """

    /*
    // vars have to be written back, but the return result of the tree should be returned at the end of the execution
    // implement this in some other way
    ..${for ((sym, select) <- normalizer.selects.toSeq
                 if (select.symbol.asTerm.isGetter && !select.symbol.asTerm.isStable)) yield {
      q"${select} = ${TermName(s"__this$$${sym.name}")}"
    }}
    */
    newCode
  }

  /**
   * Substitutes the names of classes to fully qualified names.
   * @param root expression
   * @tparam T type
   * @return Expr[T] substituted expression
   */
  def substituteClassNames[T: c.WeakTypeTag](root: Tree): Tree = {
    val qualifier = new SymbolQualifier
    val newCode = qualifier.transform(root)
    newCode
  }

  /**
   * Creates a select chain for a class. Example: Select(Select(Ident("...", ...
   * @param sym root symbol
   * @param apply set to true if a function is applied on the symbol
   * @return Tree with select chain
   */
  def mkSelect(sym: Symbol, apply: Boolean): Tree =
    if (sym.owner != c.mirror.RootClass) {
      val newSymbol: Name =
        if (apply)
          sym.name.toTermName
        else {
          if (!sym.isPackage)
            sym.name.toTypeName
          else
            sym.name.toTermName
        }

      Select(mkSelect(sym.owner, apply), newSymbol)
    } else {
      Ident(c.mirror.staticPackage(sym.fullName))
    }

  /**
   * Identify if the symbol is a parameter of a function or a val or var
   * @param variable
   * @return
   */
  def isParam(variable: Symbol): Boolean = {
    return (variable.isTerm && (variable.asTerm.isVal || variable.asTerm.isVar)) || (variable.owner.isMethod && variable.isParameter)
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

  private class SymbolQualifier extends Transformer {

    override def transform(t: Tree) = {
      t match {
        //search for a class call without 'new'
        case a@Apply(Select(i@Ident(_: TermName), termName), args) if !isParam(i.symbol) =>
          val selectChain = mkSelect(i.symbol, true)
          val result = Apply(Select(selectChain, termName), args.map(super.transform _))
          result
        //search for a class call with 'new'
        case a@Apply(Select(n@New(i@Ident(_: TypeName)), termNames.CONSTRUCTOR), args) if !i.symbol.owner.isTerm =>
          val selectChain = mkSelect(i.symbol, false)
          val result = Apply(Select(New(selectChain), termNames.CONSTRUCTOR), args.map(super.transform _))
          result
        case _ => super.transform(t)
      }
    }
  }

}
