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
package compiler.lib

import api.emma
import compiler.Common
import compiler.Compiler
import util.Memo

import shapeless._

/** Basic support for library functions inlining & normalization. */
private[compiler] trait Lib extends Common {
  self: Compiler =>

  import Registry.withAST
  import Source.{Lang => src}
  import UniverseImplicits._

  object Lib {

    // -------------------------------------------------------------------------
    // API
    // -------------------------------------------------------------------------

    lazy val expand = TreeTransform("Lib.expand",
      tree => doExpand(tree, api.Owner.encl, Nil))

    // -------------------------------------------------------------------------
    // Helper methods & objects
    // -------------------------------------------------------------------------

    private def doExpand(tree: u.Tree, owner: u.Symbol, trace: List[u.MethodSymbol]): u.Tree = {
      val rslt = api.TopDown.withOwner.transformWith({
        case Attr.inh(call@src.DefCall(Some(src.Ref(_)), sym withAST ast, targs, argss), own :: _) =>
          ensureAcyclic(trace, sym, call.pos)
          doExpand(betaReduce(ast, targs, argss), own, sym :: trace)
      })._tree(tree)
      api.Owner.at(owner)(rslt)
    }

    private def ensureAcyclic(trace: List[u.MethodSymbol], sym: u.MethodSymbol, pos: u.Position): Unit =
      if (trace contains sym) abort(s"Cyclic @emma.lib method calls:\n${printCycle(sym :: trace)}", pos)

    private def printCycle(cycle: Seq[u.MethodSymbol]): String =
      cycle.reverse.sliding(2).map({
        case Seq(callee, caller) => s" - $callee calls $caller"
      }).mkString("\n")

    private def betaReduce(defDef: u.DefDef, targs: Seq[u.Type], argss: Seq[Seq[u.Tree]]): u.Tree = {
      val api.DefDef(sym, tparams, paramss, body) = defDef

      val bndDefs = api.Tree.bindings(defDef)
      val parDefs = paramss.flatten.map(_.symbol.asTerm).toSet

      // compute prefix of ValDefs derived from ParDefs and their bound arguments
      val parDefsPrefix = for {
        (src.ParDef(p, _), rhs) <- paramss.flatten zip argss.flatten
      } yield rhs match {
        // carry over singleton types if argument is a reference
        // this is needed as rhs.tpe != y.info if y.info is a singleton type
        case src.Ref(y) =>
          val x = api.TermSym(
            own = sym,
            nme = api.TermName.fresh(p),
            tpe = y.info
          ).asTerm
          src.ValDef(x, src.Ref(y))
        case _ =>
          val x = api.TermSym(
            own = sym,
            nme = api.TermName.fresh(p),
            tpe = rhs.tpe.widen
          ).asTerm
          src.ValDef(x, rhs)
      }

      // compute type bindings substitution sequence
      val typesSubstSeq = for {
        (tp, ta) <- tparams zip targs
      } yield tp -> ta.typeSymbol.asType
      // compute type bindings substitution map
      val typesSubstMap = (for {
        (tp, ta) <- tparams zip targs
      } yield tp.toType -> ta).toMap.withDefault(t => t)

      // compute binding defs substitution sequence
      val bndDefsSubstSeq = for {
        oldSym <- (bndDefs diff parDefs).toSeq
      } yield oldSym -> api.Sym.With(oldSym)(
        nme = api.TermName.fresh(oldSym),
        tpe = oldSym.info.map(typesSubstMap),
        flg = api.Sym.flags(oldSym)
      ).asTerm

      // compute a sequence of `symbol -> tree` substitutions
      val parDefsSubstSeq = for {
        (src.ParDef(p, _), src.ValDef(x, _)) <- paramss.flatten zip parDefsPrefix
      } yield  p -> x

      ({
        api.Tree.rename(typesSubstSeq ++ bndDefsSubstSeq ++ parDefsSubstSeq, typesSubstMap)
      } andThen {
        appendPrefix(parDefsPrefix)
      } andThen {
        refreshMethodSymbols
      }) (body)
    }

    private def appendPrefix(prefix: Seq[u.ValDef])(body: u.Tree): u.Tree =
      if (prefix.isEmpty) body
      else body match {
        case src.Block(stats, expr) => src.Block(prefix ++ stats, expr)
        case expr => src.Block(prefix, expr)
      }

    private lazy val refreshMethodSymbols: u.Tree => u.Tree =
      api.BottomUp.transform({
        case api.DefCall(Some(mTgt), mSym, mTargs, mArgss) =>
          val newMethod = mTgt.tpe.member(api.TermName(mSym)).asTerm
          api.DefCall(Some(mTgt), newMethod, mTargs, mArgss)
      })._tree
  }

  /* Lirary function registry. */
  private object Registry {

    /** If the given MethodSymbol is a library function, loads it into the registry and retuns its AST. */
    def apply(sym: u.MethodSymbol): Option[u.DefDef] = getOrLoad(sym)

    /** Extractor for the AST associated with a library function symbol. */
    object withAST {
      def unapply(sym: u.MethodSymbol): Option[(u.MethodSymbol, u.DefDef)] =
        apply(sym).map(ast => (sym, ast))
    }

    /** Extractor for the `emma.src` annotation associated with a library function symbol. */
    object withAnn {
      def unapply(sym: u.MethodSymbol): Option[(u.MethodSymbol, u.Annotation)] =
        api.Sym.findAnn[emma.src](sym) match {
          case Some(ann) => Some(sym, ann)
          case None => None
        }
    }

    /** Loads and memoizes an AST for a library function from a `sym` with an `emma.src` annotation. */
    private lazy val getOrLoad = Memo[u.MethodSymbol, Option[u.DefDef]]({
      case sym withAnn ann =>
        ann.tree.collect {
          case src.Lit(fldName: String) =>
            // build an `objSym.fldSym` selection expression
            val cls = sym.owner.asClass
            val fld = cls.info.member(api.TermName(fldName)).asTerm
            val sel = qualifyStatics(api.DefCall(Some(api.Ref(cls.module)), fld))
            // evaluate `objSym.fldSym` and grab the DefDef source
            val src = eval[String](sel)
            // parse the source and grab the DefDef AST
            parse(Seq(TreeTransform("Lib.getOrLoad/collect(Lib.extractDef)", _.collect(extractDef).head),
              TreeTransform("Lib.fixDefDefSymbols", fixDefDefSymbols(sym))))(src)
        }.collectFirst(extractDef)
      case _ =>
        None
    })

    /** Extracts the first DefDef from an input tree. */
    private lazy val extractDef: u.Tree =?> u.DefDef = {
      case dd@api.DefDef(_, _, _, _) => dd
    }

    /** Parses a DefDef AST from a string. */
    private val parse = (transformations: Seq[TreeTransform]) =>
      pipeline(typeCheck = true, withPost = false)(
        transformations: _*
      ).compose(self.parse)

    /** Replaces the top-level DefDef symbol of the quoted tree with the corrsponding original symbol. */
    private val fixDefDefSymbols = (original: u.MethodSymbol) =>
      api.TopDown.break.transform {
        case api.DefDef(_, tparams, paramss, body) =>
          api.DefDef(original, tparams, paramss.map(_.map(_.symbol.asTerm)), body)
      } andThen (_.tree)
  }

}
