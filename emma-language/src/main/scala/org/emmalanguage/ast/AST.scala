/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package ast

import shapeless._

import scala.annotation.tailrec

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

  import universe._

  /** Virtual non-overlapping semantic AST based on Scala trees. */
  object api extends API

  /**
   * Populates the missing types of lambda symbols in a tree.
   * WARN: Mutates the symbol table in place.
   */
  lazy val fixSymbolTypes: u.Tree => u.Tree =
    api.TopDown.traverse {
      case api.Lambda(f, _, _) withType fT => set.tpe(f, fT)
      case u.DefDef(_, _, tparams, paramss, _ withType tpe, _) withSym method =>
        val tps = tparams.map(_.symbol.asType)
        val pss = paramss.map(_.map(_.symbol.asTerm))
        val Res = tpe.finalResultType
        set.tpe(method, api.Type.method(tps: _*)(pss: _*)(Res))
    }.andThen(_.tree)

  /**
   * Replaces [[u.TypeTree]]s that have their `original` field set with stubs that only have their
   * `tpe` field set to the corresponding type. Type-trees of `val/var`s are left empty for the
   * compiler to infer.
   */
  lazy val stubTypeTrees: u.Tree => u.Tree =
    api.TopDown.break.withParent.transformWith {
      // Leave `val/var` types to be inferred by the compiler.
      case Attr.inh(u.TypeTree(), Some(api.BindingDef(lhs, rhs, _)) :: _)
        if !lhs.isParameter && rhs.nonEmpty => u.TypeTree()
      case Attr.none(u.TypeTree() withType tpe) => api.TypeQuote(tpe)
    }.andThen(_.tree)

  /** Restores [[u.TypeTree]]s with their `original` field set. */
  lazy val restoreTypeTrees: u.Tree => u.Tree =
    api.TopDown.break.transform {
      case u.TypeTree() withType tpe => api.Type.tree(tpe)
    }.andThen(_.tree)

  /** Normalizes all statements in term position by wrapping them in a block. */
  lazy val normalizeStatements: u.Tree => u.Tree =
    api.BottomUp.withParent.transformWith {
      case Attr.inh(mol @ (api.VarMut(_, _) | api.Loop(_, _)), Some(_: u.Block) :: _) =>
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
  lazy val unQualifyStatics: u.Tree => u.Tree =
    api.TopDown.break.transform {
      case api.Sel(_, sym) if sym.isStatic && (sym.isClass || sym.isModule) =>
        api.Id(sym)
    }.andThen(_.tree)

  /** Fully qualifies references to static symbols. */
  lazy val qualifyStatics: u.Tree => u.Tree =
    api.TopDown.break.transform {
      case api.Ref(sym) if sym.isStatic && (sym.isClass || sym.isModule) =>
        api.Tree.resolveStatic(sym)
    }.andThen(_.tree)

  /** Ensures that all definitions within `tree` have unique names. */
  lazy val resolveNameClashes: u.Tree => u.Tree = (tree: u.Tree) => {
    val defs = api.Tree.defs(tree)   // definitions in the given `tree`
    val notUnique = defs.map(_.name) // names already used in the given `tree`
    val clashes = nameClashes(defs)  // name clashes in the given `tree`

    // Helper method: gets the first fresh name that does not collide
    // with another def in the given tree.
    @tailrec def fresh(nme: u.TermName): u.TermName = {
      val fsh = api.TermName.fresh(nme)
      if (notUnique(fsh)) fresh(nme) else fsh
    }

    // Custom build aliases using the helper `fresh` method.
    val aliases = for (sym <- clashes) yield
      sym -> api.Sym.copy(sym)(name = fresh(sym.name)).asTerm

    api.Tree.rename(aliases: _*)(tree)
  }

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
  def asSource(title: String)(tree: u.Tree): String = {
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

  /** Returns a sequence of symbols in `tree` that have clashing names. */
  def nameClashes(tree: u.Tree): Seq[u.TermSymbol] =
    nameClashes(api.Tree.defs(tree))

  /** Returns a sequence of symbols in `defs` that have clashing names. */
  private def nameClashes(defs: Set[u.TermSymbol]): Seq[u.TermSymbol] = for {
    (_, defs) <- defs.groupBy(_.name).toSeq
    if defs.size > 1
    dfn <- defs.tail
  } yield dfn

  private[ast] def freshNameSuffix: Char
}
