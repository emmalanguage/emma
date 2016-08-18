package eu.stratosphere
package emma.ast

import shapeless._

/** Common super-trait for macro- and runtime-compilation. */
trait AST extends CommonAST
  with Bindings
  with Loops
  with Methods
  with Modules
  with Parameters
  with Patterns
  with Symbols
  with Terms
  with Transversers
  with Trees
  with Types
  with Values
  with Variables {

  /** Virtual AST API. */
  trait API
    extends BindingAPI
    with LoopAPI
    with MethodAPI
    with ModuleAPI
    with ParameterAPI
    with PatternAPI
    with SymbolAPI
    with TermAPI
    with TransverserAPI
    with TreeAPI
    with TypeAPI
    with ValueAPI
    with VariableAPI

  /** Virtual non-overlapping semantic AST based on Scala trees. */
  object api extends API
  import universe._

  /**
   * Populates the missing types of lambda symbols in a tree.
   * WARN: Mutates the symbol table in place.
   */
  lazy val fixLambdaTypes: u.Tree => u.Tree =
    api.BottomUp.traverse {
      case api.Lambda(f, _, _) withType fT => set.tpe(f, fT)
    }.andThen(_.tree)

  /** Normalizes all statements in term position by wrapping them in a block. */
  lazy val normalizeStatements: u.Tree => u.Tree =
    api.BottomUp.withParent.transformWith {
      case Attr.inh(mol @ (api.VarMut(_, _) | api.Loop(_, _)), (_: u.Block) :: _) =>
        mol
      case Attr.none(mol @ (api.VarMut(_, _) | api.Loop(_, _))) =>
        api.Block(mol)()
      case Attr.none(u.Block(stats, mol @ (api.VarMut(_, _) | api.Loop(_, _)))) =>
        api.Block(stats :+ mol: _*)()
      case Attr.none(norm @ api.WhileBody(_, _, api.Block(_, api.Lit(())))) =>
        norm
      case Attr.none(norm @ api.DoWhileBody(_, _, api.Block(_, api.Lit(())))) =>
        norm
      case Attr.none(api.WhileBody(label, cond, api.Block(stats, stat))) =>
        api.WhileBody(label, cond, api.Block(stats :+ stat: _*)())
      case Attr.none(api.DoWhileBody(label, cond, api.Block(stats, stat))) =>
        api.DoWhileBody(label, cond, api.Block(stats :+ stat: _*)())
      case Attr.none(api.WhileBody(label, cond, stat)) =>
        api.WhileBody(label, cond, api.Block(stat)())
      case Attr.none(api.DoWhileBody(label, cond, stat)) =>
        api.DoWhileBody(label, cond, api.Block(stat)())
    }.andThen(_.tree)

  /** Removes the qualifiers from references to static symbols. */
  lazy val unQualifyStaticModules: u.Tree => u.Tree =
    api.TopDown.break.transform {
      case api.Sel(_, api.ModuleSym(mod)) if mod.isStatic =>
        api.Id(mod)
    }.andThen(_.tree)

  /** Fully qualifies references to static symbols. */
  lazy val qualifyStaticModules: u.Tree => u.Tree =
    api.TopDown.break.transform {
      case api.TermRef(api.ModuleSym(mod)) if mod.isStatic =>
        api.Tree.resolveStatic(mod)
    }.andThen(_.tree)

  /**
   * Prints `tree` for debugging.
   *
   * Makes a best effort to shorten the resulting source code for better readability, especially
   * removing particular package qualifiers. Does not return parseable source code.
   *
   * @param title Useful to distinguish different printouts from each other.
   * @param tree The tree to print as source code.
   * @return The printable source code.
   */
  override def asSource(title: String)(tree: u.Tree): String = {
    val sb = StringBuilder.newBuilder
    // Prefix
    sb.append(title)
      .append("\n")
      .append("-" * 80)
      .append("\n")
    // Tree
    sb.append(u.showCode(tree)
      .replace("<synthetic> ", "")
      .replace("_root_.", "")
      .replace("eu.stratosphere.emma.", "")
      .replace("eu.stratosphere.emma.compiler.ir.`package`.", "")
      .replaceAll("eu\\.stratosphere\\.emma\\.testschema\\.([a-zA-Z]+)\\.?", ""))
      .append("\n")
    // Suffix
    sb.append("-" * 80)
      .append("\n")
    // Grab the result
    sb.result()
  }
}
