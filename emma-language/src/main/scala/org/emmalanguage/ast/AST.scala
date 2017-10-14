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

/** Common super-trait for macro- and runtime-compilation. */
trait AST extends CommonAST
  with Bindings
  with Loops
  with Methods
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
    with ParameterAPI
    with PatternAPI
    with SymbolAPI
    with TermAPI
    with TransverserAPI
    with TreeAPI
    with TypeAPI
    with ValueAPI
    with VariableAPI

  import u._
  import internal._
  import reificationSupport._

  /** Virtual non-overlapping semantic AST based on Scala trees. */
  object api extends API

  /**
   * Populates the missing types of lambda symbols in a tree.
   * WARN: Mutates the symbol table in place.
   */
  lazy val fixSymbolTypes = TreeTransform("AST.fixSymbolTypes", api.TopDown.traverse {
    case lambda @ api.Lambda(fun, _, _) =>
      setInfo(fun, lambda.tpe)
    case api.DefDef(method, tparams, paramss, _) =>
      val pss = paramss.map(_.map(_.symbol.asTerm))
      val res = method.info.finalResultType
      setInfo(method, api.Type.method(tparams, pss, res))
  }.andThen(_.tree))

  /**
   * Replaces [[u.TypeTree]]s that have their `original` field set with stubs that only have their
   * `tpe` field set to the corresponding type. Type-trees of `val/var`s are left empty for the
   * compiler to infer.
   */
  lazy val stubTypeTrees = TreeTransform("AST.stubTypeTrees", api.TopDown.break
    .withParent.transformWith {
      // Leave `val/var` types to be inferred by the compiler.
      case Attr.inh(api.TypeQuote(tpe), Some(api.BindingDef(lhs, rhs)) :: _)
        if !lhs.isParameter && rhs.nonEmpty && tpe =:= rhs.tpe.dealias.widen =>
        api.TypeQuote.empty
      case Attr.none(api.TypeQuote(tpe)) =>
        api.TypeQuote(tpe)
    }.andThen(_.tree))

  /** Restores [[u.TypeTree]]s with their `original` field set. */
  lazy val restoreTypeTrees = TreeTransform("AST.restoreTypeTrees", api.TopDown.break.transform {
    case api.TypeQuote(tpe) => api.Type.tree(tpe)
  }.andThen(_.tree))

  /**
   * Rewrites `A.this.x` as `x` if `A` is shadowed another symbol in the owner chain.
   *
   * Can be used before `showCode` to avoid printing invalid trees such as
   *
   * {{{
   * class A {
   *   val x = 42;
   *   class A {
   *     scala.Predef.println(A.this.x)
   *   }
   * }
   * }}}
   */
  lazy val removeShadowedThis = TreeTransform("AST.removeShadowedThis",
    if (shadowedOwners.isEmpty) identity[u.Tree] _
    else api.TopDown.transform {
      case api.BindingDef(x, api.Sel(api.This(encl), member))
        if shadowedOwners(encl) =>
        setInfo(x, x.info.widen)
        api.BindingDef(x, api.Id(member))
      case api.Sel(api.This(encl), member)
        if shadowedOwners(encl) => api.Id(member)
    }.andThen(_.tree))

  /** Computes a set of owners whose name is shadowed in the current scope. */
  lazy val shadowedOwners = api.Owner.chain(api.Owner.encl)
    .filter(_.isClass).groupBy(_.name).values.flatMap(_.tail).toSet

  /** Normalizes all statements in term position by wrapping them in a block. */
  lazy val normalizeStatements = TreeTransform("AST.normalizeStatements", {
    def isStat(tree: u.Tree) = tree match {
      case api.VarMut(_, _) => true
      case api.Loop(_, _)   => true
      case _ => false
    }

    api.BottomUp.withParent.transformWith {
      case Attr.inh(tree, Some(_: u.Block) :: _)
        if isStat(tree) => tree
      case Attr.none(tree)
        if isStat(tree) => api.Block(Seq(tree))
      case Attr.none(u.Block(stats, expr))
        if isStat(expr) => api.Block(stats :+ expr)
      case Attr.none(body @ api.WhileBody(_, _, api.Block(_, api.Lit(())))) => body
      case Attr.none(body @ api.DoWhileBody(_, _, api.Block(_, api.Lit(())))) => body
      case Attr.none(api.WhileBody(lbl, cond, api.Block(stats, stat))) =>
        api.WhileBody(lbl, cond, api.Block(stats :+ stat))
      case Attr.none(api.DoWhileBody(lbl, cond, api.Block(stats, stat))) =>
        api.DoWhileBody(lbl, cond, api.Block(stats :+ stat))
      case Attr.none(api.WhileBody(lbl, cond, stat)) =>
        api.WhileBody(lbl, cond, api.Block(Seq(stat)))
      case Attr.none(api.DoWhileBody(lbl, cond, stat)) =>
        api.DoWhileBody(lbl, cond, api.Block(Seq(stat)))
    }.andThen(_.tree)
  })

  /** Removes the qualifiers from references to static symbols. */
  lazy val unQualifyStatics = TreeTransform("AST.unQualifyStatics", api.TopDown.break.transform {
    case api.Sel(_, member) if member.isStatic && (member.isClass || member.isModule) =>
      api.Id(member)
  }.andThen(_.tree))

  /** Fully qualifies references to static symbols. */
  lazy val qualifyStatics = TreeTransform("AST.qualifyStatics", api.TopDown.break.transform {
    case api.Ref(target) if target.isStatic && (target.isClass || target.isModule) =>
      api.Tree.resolveStatic(target)
  }.andThen(_.tree))

  /** Ensures that all definitions within `tree` have unique names. */
  lazy val resolveNameClashes = TreeTransform("AST.resolveNameClashes",
    tree => api.Tree.refresh(nameClashes(tree))(tree))

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
  def nameClashes(tree: u.Tree): Seq[u.TermSymbol] = for {
    (_, defs) <- api.Tree.defs(tree).groupBy(_.name).toSeq
    if defs.size > 1
    dfn <- defs.tail
  } yield dfn
}
