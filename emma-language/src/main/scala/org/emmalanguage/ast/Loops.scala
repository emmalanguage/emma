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

/** Loops (while and do-while). */
trait Loops { this: AST =>

  /** Loops (while and do-while). */
  trait LoopAPI { this: API =>

    import u._
    import internal._
    import reificationSupport._
    import Flag._

    /** Converts `tree` to a statement (block with `Unit` expression). */
    private def asStat(tree: u.Tree) = tree match {
      case Block(_, Lit(())) => tree
      case u.Block(stats, stat) => Block(stats :+ stat)
      case _ => Block(Seq(tree))
    }

    /** Label (loop) symbols. */
    object LabelSym extends Node {

      /**
       * Creates a type-checked label symbol.
       * @param own The enclosing named entity where this label is defined.
       * @param nme The name of this label (will be encoded).
       * @param pos The (optional) source code position where this label is defined.
       * @return A new type-checked label symbol.
       */
      def apply(own: u.Symbol, nme: u.TermName,
        pos: u.Position = u.NoPosition
      ): u.MethodSymbol = {
        val lbl = newMethodSymbol(own, TermName(nme), pos, CONTRAVARIANT)
        setInfo(lbl, Type.loop)
      }

      def unapply(sym: u.MethodSymbol): Option[u.MethodSymbol] =
        Option(sym).filter(is.label)
    }

    /** Extractor for loops (while and do-while). */
    object Loop extends Node {
      def unapply(loop: u.LabelDef): Option[(u.Tree, u.Tree)] = loop match {
        case While(cond, body)   => Some(cond, body)
        case DoWhile(cond, body) => Some(cond, body)
        case _ => None
      }
    }

    /** While loops. */
    object While extends Node {

      /**
       * Creates a type-checked while loop.
       * @param cond The loop condition (must be a boolean term).
       * @param body The loop body block.
       * @return `while (cond) { body }`.
       */
      def apply(cond: u.Tree, body: u.Tree): u.LabelDef = {
        assert(is.defined(cond),       s"$this condition is not defined")
        assert(is.defined(body),       s"$this body is not defined")
        assert(has.tpe(cond),          s"$this condition has no type:\n${Tree.showTypes(cond)}")
        assert(cond.tpe <:< Type.bool, s"$this condition is not boolean:\n${Tree.showTypes(cond)}")
        val nme = TermName.fresh("while")
        val lbl = LabelSym(Sym.none, nme)
        val rhs = WhileBody(lbl, cond, asStat(body))
        val dfn = u.LabelDef(nme, Nil, rhs)
        setSymbol(dfn, lbl)
        setType(dfn, Type.unit)
      }

      def unapply(loop: u.LabelDef): Option[(u.Tree, u.Tree)] = loop match {
        case u.LabelDef(_, Nil, WhileBody(_, cond, body)) => Some(cond, body)
        case _ => None
      }
    }

    /** Do-while loops. */
    object DoWhile extends Node {

      /**
       * Creates a type-checked do-while loop.
       * @param cond The loop condition (must be a boolean term).
       * @param body The loop body block.
       * @return `do { body } while (cond)`.
       */
      def apply(cond: u.Tree, body: u.Tree): u.LabelDef = {
        assert(is.defined(cond),       s"$this condition is not defined")
        assert(is.defined(body),       s"$this body is not defined")
        assert(has.tpe(cond),          s"$this condition has no type:\n${Tree.showTypes(cond)}")
        assert(cond.tpe <:< Type.bool, s"$this condition is not boolean:\n${Tree.showTypes(cond)}")
        val nme = TermName.fresh("doWhile")
        val lbl = LabelSym(Sym.none, nme)
        val rhs = DoWhileBody(lbl, cond, asStat(body))
        val dfn = u.LabelDef(nme, Nil, rhs)
        setSymbol(dfn, lbl)
        setType(dfn, Type.unit)
      }

      def unapply(loop: u.LabelDef): Option[(u.Tree, u.Tree)] = loop match {
        case u.LabelDef(_, Nil, DoWhileBody(_, cond, body)) => Some(cond, body)
        case _ => None
      }
    }

    /** Helper object for while loop body blocks. */
    private[ast] object WhileBody extends Node {

      /**
       * Creates a type-checked `while` body.
       * @param lbl  The label symbol.
       * @param cond The loop condition (must be a boolean term).
       * @param stat The loop statement.
       * @return `if (cond) { stat; while$n() } else ()`.
       */
      def apply(lbl: u.MethodSymbol, cond: u.Tree, stat: u.Tree): u.If = {
        assert(is.defined(lbl), s"$this label is not defined")
        assert(is.label(lbl),   s"$this label $lbl is not a label")
        Branch(cond, Block(Seq(stat), LoopCall(lbl)))
      }

      def unapply(body: u.If): Option[(u.MethodSymbol, u.Tree, u.Tree)] = body match {
        case u.If(Term(cond), u.Block(stat :: Nil, LoopCall(lbl)), Lit(())) =>
          Some(lbl, cond, stat)
        case _ => None
      }
    }

    /** Helper object for do-while loop body blocks. */
    private[ast] object DoWhileBody extends Node {

      /**
       * Creates a type-checked `do-while` body.
       * @param lbl  The label symbol.
       * @param cond The loop condition (must be a boolean term).
       * @param stat The loop statement.
       * @return `{ stat; if (cond) while$n() else () }`.
       */
      def apply(lbl: u.MethodSymbol, cond: u.Tree, stat: u.Tree): u.Block = {
        assert(is.defined(lbl), s"$this label is not defined")
        assert(is.label(lbl),   s"$this label $lbl is not a label")
        Block(Seq(stat), Branch(cond, LoopCall(lbl)))
      }

      def unapply(body: u.Block): Option[(u.MethodSymbol, u.Tree, u.Tree)] = body match {
        case u.Block(stat :: Nil, u.If(Term(cond), LoopCall(lbl), Lit(()))) =>
          Some(lbl, cond, stat)
        case _ => None
      }
    }

    /** Helper object for loop bodies. */
    private[ast] object LoopCall extends Node {

      def apply(lbl: u.MethodSymbol): u.Tree = {
        assert(is.defined(lbl), s"$this label is not defined")
        assert(is.label(lbl),   s"$this label $lbl is not a label")
        TermApp(Id(lbl), argss = Seq(Seq.empty))
      }

      def unapply(call: u.Apply): Option[u.MethodSymbol] = call match {
        case TermApp(Id(LabelSym(lbl)), _, _) => Some(lbl)
        case _ => None
      }
    }
  }
}
