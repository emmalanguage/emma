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

import util.Functions
import util.Functions._
import util.Memo
import util.Monoids._

import cats.Monoid
import cats.std.all._
import cats.syntax.group._
import shapeless._
import shapeless.labelled._
import shapeless.ops.record.Selector
import shapeless.syntax.singleton._

import scala.Function.const
import scala.annotation.implicitNotFound
import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.language.higherKinds

/** Utility for traversing and transforming trees. */
trait Transversers { this: AST =>

  import universe._

  /**
   * Placeholder for attribute grammars.
   * @param accumulation Template for accumulated (along the traversal path) attributes.
   * @param inheritance Template for inherited (top-down) attributes.
   * @param synthesis Template for synthesized (bottom-up) attributes.
   * @param MAcc A monoid for accumulated attributes.
   * @param MInh A monoid for inherited attributes.
   * @param MSyn A monoid for synthesized attributes.
   * @tparam A The types of accumulated attributes.
   * @tparam I The types of inherited attributes.
   * @tparam S The types of synthesized attributes.
   */
  private[ast] case class AttrGrammar[A <: HList, I <: HList, S <: HList](
      initAcc: A, initInh: I, initSyn: S,
      accumulation: Attr[A, I, S] => A,
      inheritance:  Attr[HNil, I, S] => I,
      synthesis:    Attr[HNil, HNil, S] => S
  )(implicit
    val MAcc: Monoid[A],
    val MInh: Monoid[I],
    val MSyn: Monoid[S]
  ) {

    /** Prepends an accumulated (along the traversal path) attribute. */
    def accumulate[X](init: X, acc: Attr[X :: A, I, S] =?> X)(implicit M: Monoid[X]) =
      copy[X :: A, I, S](initAcc = init :: initAcc,
        accumulation = { case attr @ Attr.acc(_, a :: as) =>
          val as1 = accumulation(attr.copy(acc = as))
          val as2 = a :: MAcc.combine(as, as1)
          complete(acc)(attr.copy(acc = as2))(M.empty) :: as1
      })

    /** Prepends an inherited (from parents) attribute. */
    def inherit[X](init: X, inh: Attr[HNil, X :: I, S] =?> X)(implicit M: Monoid[X]) =
      copy[A, X :: I, S](initInh = init :: initInh,
        accumulation = attr => accumulation(attr.copy(inh = attr.inh.tail)),
        inheritance  = { case attr @ Attr.inh(_, i :: is) =>
          val is1 = inheritance(attr.copy(inh = is))
          val is2 = i :: MInh.combine(is, is1)
          complete(inh)(attr.copy(inh = is2))(M.empty) :: is1
        })

    /** Prepends a synthesized (from children) attribute. */
    def synthesize[X](init: X, syn: Attr[HNil, HNil, X :: S] =?> X)(implicit M: Monoid[X]) = {
      copy[A, I, X :: S](initSyn = init :: initSyn,
        accumulation = attr => accumulation(attr.copy(syn = attr.syn.tail)),
        inheritance  = attr => inheritance(attr.copy(syn = attr.syn.tail)),
        synthesis = { case attr @ Attr.syn(_, s :: ss) =>
          val ss1 = synthesis(attr.copy(syn = ss))
          val ss2 = s :: MSyn.combine(ss, ss1)
          complete(syn)(attr.copy(syn = ss2))(M.empty) :: ss1
        })
    }
  }

  /** Utility for managing attribute grammars. */
  trait ManagedAttr[A <: HList, I <: HList, S <: HList] extends (Tree => Attr[A, I, S]) {

    // Expose type arguments.
    type Acc = A
    type Inh = I
    type Syn = S

    protected val grammar: AttrGrammar[A, I, S]
    val callback: Attr[A, I, S] =?> Unit = PartialFunction.empty
    val template: Attr[A, I, S] =?> Tree = PartialFunction.empty

    // Expose monoid instances.
    implicit def MAcc = grammar.MAcc // accumulation monoid
    implicit def MInh = grammar.MInh // inheritance monoid
    implicit def MSyn = grammar.MSyn // synthesis monoid

    // Shorthand forwarders.
    final def _tree: Tree => Tree = andThen(_.tree)
    final def _acc:  Tree => A    = andThen(_.acc)
    final def _inh:  Tree => I    = andThen(_.inh)
    final def _syn:  Tree => S    = andThen(_.syn)

    /** Accumulated state. */
    private var state = grammar.initAcc

    /** Inherited state. */
    private var stack = grammar.initInh :: Nil

    /** Synthesized state. */
    private val cache = mutable.Map.empty[Tree, S]

    /** Get all accumulated attributes at the current tree. */
    private def acc: A = state

    /** Get all inherited attributes at the current tree. */
    private def inh: I = stack.head

    /** A function from tree to synthesized attributes. */
    final lazy val syn: Tree => S =
      if (grammar.initSyn == HNil) const(grammar.initSyn)
      else Memo.recur[Tree, S]({ syn => tree =>
        val children = tree.children
        val prev = if (children.isEmpty) grammar.initSyn
          else MSyn.combineAll(children.map(syn))
        val curr = grammar.synthesis(Attr(tree, prev))
        MSyn.combine(prev, curr)
      }, cache)

    /** Annotates `tree` with all attributes. */
    protected final def ann(tree: Tree): Attr[A, I, S] =
      Attr(tree, acc, inh, syn(tree))

    protected final lazy val accumulation: Tree => A =
      grammar.accumulation.compose(ann)

    protected final lazy val inheritance: Tree => I =
      grammar.inheritance.compose(tree => Attr(tree, inh, syn(tree)))

    protected final lazy val traversal: Tree =?> Unit =
      Functions.compose(callback)(ann)

    protected final lazy val transformation: Tree =?> Tree =
      Functions.compose(template)(ann)

    /** Inherit attributes for `tree`. */
    protected final def at[X](tree: Tree)(f: => X): X =
      if (inh == HNil) f else {
        stack ::= inh |+| inheritance(tree)
        val result = f
        stack = stack.tail
        result
      }

    /** Accumulate attributes for `tree`. */
    protected final def accumulate(tree: Tree): Unit =
      if (acc != HNil) state = acc |+| accumulation(tree)

    /** Resets the state so that another tree can be traversed. */
    protected def reset(): Unit = {
      state = grammar.initAcc
      stack = grammar.initInh :: Nil
      cache.clear()
    }

    def prepend[L <: HList]=
      ops.hlist.Prepend[Int :: HNil, L]
  }

  /**
   * A traversal / transformation strategy.
   * 4 fundamental strategies are available:
   *
   * 1. Top-down continue left-to-right
   * 2. Top-down break left-to-right
   * 3. Bottom-up continue left-to-right
   * 4. Bottom-up break left-to-right
   *
   * Right-to-left variants are not supported.
   */
  case class Strategy[A <: HList, I <: HList, S <: HList](
      private val grammar: AttrGrammar[A, I, S], private val factory: Factory) {

    // Expose type arguments.
    type Acc = A
    type Inh = I
    type Syn = S

    /** Prepends an accumulated attribute based on all attributes. */
    def accumulateInit[X: Monoid](init: X)(acc: Attr[X :: A, I, S] =?> X) =
      copy(grammar = grammar.accumulate(init, acc))

    /** Prepends an accumulated attribute based on all attributes. */
    def accumulateWith[X](acc: Attr[X :: A, I, S] =?> X)(implicit m: Monoid[X]) =
      accumulateInit(m.empty)(acc)

    /** Prepends an accumulated attribute based on trees only. */
    def accumulate[X: Monoid](acc: Tree =?> X) =
      accumulateWith[X](forgetful(acc))

    /** Prepends an accumulated attribute. */
    def accum[K, V](attr: K ~@ V)(acc: Attr[FieldType[K, V] :: A, I, S] =?> V) =
      accumulateInit(attr.init)(acc.andThen(field[K](_)))(kv(attr.M))

    /** Prepends an inherited attribute based on inherited and synthesized attributes. */
    def inheritInit[X: Monoid](init: X)(inh: Attr[HNil, X :: I, S] =?> X) =
      copy(grammar = grammar.inherit(init, inh))

    /** Prepends an inherited attribute based on inherited and synthesized attributes. */
    def inheritWith[X](inh: Attr[HNil, X :: I, S] =?> X)(implicit m: Monoid[X]) =
      inheritInit(m.empty)(inh)

    /** Prepends an inherited attribute based on trees only. */
    def inherit[X: Monoid](inh: Tree =?> X) =
      inheritWith[X](forgetful(inh))

    /** Prepends an inherited attribute. */
    def inher[K, V](attr: K ~@ V)(inh: Attr[HNil, FieldType[K, V] :: I, S] =?> V) =
      inheritInit(attr.init)(inh.andThen(field[K](_)))(kv(attr.M))

    /** Prepends a synthesized attribute based on all synthesized attributes. */
    def synthesizeInit[X: Monoid](init: X)(syn: Attr[HNil, HNil, X :: S] =?> X) =
      copy(grammar = grammar.synthesize(init, syn))

    /** Prepends a synthesized attribute based on all synthesized attributes. */
    def synthesizeWith[X](syn: Attr[HNil, HNil, X :: S] =?> X)(implicit m: Monoid[X]) =
      synthesizeInit(m.empty)(syn)

    /** Prepends a synthesized attribute based on trees only. */
    def synthesize[X: Monoid](syn: Tree =?> X) =
      synthesizeWith[X](forgetful(syn))

    /** Prepends a synthesized attribute. */
    def synth[K, V](attr: K ~@ V)(syn: Attr[HNil, HNil, FieldType[K, V] :: S] =?> V) =
      synthesizeInit(attr.init)(syn.andThen(field[K](_)))(kv(attr.M))

    /** Traverses a tree with access to all attributes (and a memoized synthesis function). */
    def traverseSyn(callback: Attr[A, I, Tree => S] =?> Unit): Traversal[A, I, S] = {
      lazy val traversal: Traversal[A, I, S] = traverseWith {
        case Attr(t, as, is, _) if callback.isDefinedAt(Attr(t, as, is, traversal.syn)) =>
          callback(Attr(t, as, is, traversal.syn))
      }

      traversal
    }

    /** Traverses a tree with access to all attributes. */
    def traverseWith(callback: Attr[A, I, S] =?> Unit): Traversal[A, I, S] =
      factory.traversal(grammar)(callback)

    /** Traverses a tree without access to attributes. */
    def traverse(callback: Tree =?> Unit): Traversal[A, I, S] =
      traverseWith(forgetful(callback))

    /** Shortcut for visiting every node in a tree. */
    def traverseAny: Traversal[A, I, S] =
      factory.traversal(grammar) { case _ => () }

    /** Transforms a tree with access to all attributes (and a memoized synthesis function). */
    def transformSyn(template: Attr[A, I, Tree => S] =?> Tree): Transform[A, I, S] = {
      lazy val transform: Transform[A, I, S] = transformWith {
        case Attr(t, as, is, _) if template.isDefinedAt(Attr(t, as, is, transform.syn)) =>
          template(Attr(t, as, is, transform.syn))
      }

      transform
    }

    /** Transforms a tree with access to all attributes. */
    def transformWith(template: Attr[A, I, S] =?> Tree): Transform[A, I, S] =
      factory.transform(grammar)(template)

    /** Transforms a tree without access to attributes. */
    def transform(template: Tree =?> Tree): Transform[A, I, S] =
      transformWith(forgetful(template))

    /** Inherits the root of the tree ([[None]] if the current node is the root). */
    def withRoot = inher(Attr.Root) {
      case tree ~@ _ => Option(tree)
    }

    /** Inherits the parent of the current node ([[None]] if the current node is the root). */
    def withParent = inher(Attr.Parent) {
      case t ~@ _ => Option(t)
    }

    /** Inherits all ancestors of the current node in a vector. */
    def withAncestors = inher(Attr.Ancestors) {
      case t ~@ _ => Vector(t)
    }

    /** Inherits the owner of the current node. */
    def withOwner(default: Symbol = Attr.Owner.init) = {
      val Owner = ~@('owner ->> default)(Attr.Owner.M)
      inher(Owner) { case api.Owner(o) ~@ _ => o }
    }

    /** Inherits the owner chain of the current node. */
    def withOwnerChain = inher(Attr.OwnerChain) {
      case api.Owner(o) ~@ _ => Vector(o)
    }

    /** Synthesizes all term definitions contained in the current node and its children. */
    def withDefs = synth(Attr.Defs) {
      case (d @ api.TermDef(x)) ~@ _ => Map(x -> d)
    }

    /** Synthesizes all binding definitions contained in the current node and its children. */
    def withBindDefs = synth(Attr.BindDefs) {
      case (b @ api.BindingDef(x, _)) ~@ _ => Map(x -> b)
    }

    /** Synthesizes all value definitions contained in the current node and its children. */
    def withValDefs = synth(Attr.ValDefs) {
      case (v @ api.ValDef(x, _)) ~@ _ => Map(x -> v)
    }

    /** Synthesizes all variable definitions contained in the current node and its children. */
    def withVarDefs = synth(Attr.VarDefs) {
      case (v @ api.VarDef(x, _)) ~@ _ => Map(x -> v)
    }

    /** Synthesizes all parameter definitions contained in the current node and its children. */
    def withParDefs = synth(Attr.ParDefs) {
      case (p @ api.ParDef(x, _)) ~@ _ => Map(x -> p)
    }

    /** Synthesizes all method definitions contained in the current node and its children. */
    def withDefDefs = synth(Attr.DefDefs) {
      case (d @ api.DefDef(m, _, _, _)) ~@ _ => Map(m -> d)
    }

    /** Counts all term references contained in the current node and its children. */
    def withUses = synth(Attr.Uses) {
      case api.TermRef(x) ~@ _ => Map(x -> 1)
    }

    /** Counts all binding references contained in the current node and its children. */
    def withBindUses = synth(Attr.BindUses) {
      case api.BindingRef(x) ~@ _ => Map(x -> 1)
    }

    /** Counts all value references contained in the current node and its children. */
    def withValUses = synth(Attr.ValUses) {
      case api.ValRef(x) ~@ _ => Map(x -> 1)
    }

    /** Counts all variable references contained in the current node and its children. */
    def withVarUses = synth(Attr.VarUses) {
      case api.VarRef(x) ~@ _ => Map(x -> 1)
    }

    /** Counts all parameter references contained in the current node and its children. */
    def withParUses = synth(Attr.ParUses) {
      case api.ParRef(x) ~@ _ => Map(x -> 1)
    }

    /** Counts all variable assignments contained in the current node and its children. */
    def withAssigns = synth(Attr.Assigns) {
      case api.VarMut(x, _) ~@ _ => Map(x -> 1)
    }

    /** Counts all method calls contained in the current node and its children. */
    def withDefCalls = synth(Attr.DefCalls) {
      case api.DefCall(_, m, _, _) ~@ _ => Map(m -> 1)
    }

    /** Converts a partial function over trees to a partial function over attributed trees. */
    private def forgetful[X, Acc, Inh, Syn](pf: Tree =?> X): Attr[Acc, Inh, Syn] =?> X = {
      case Attr.none(t) if pf.isDefinedAt(t) => pf(t)
    }
  }

  /** An abstract transformation (default is top-down break). */
  abstract class Transform[A <: HList, I <: HList, S <: HList](
      protected val grammar: AttrGrammar[A, I, S],
      override val template: Attr[A, I, S] =?> Tree
  ) extends Transformer with ManagedAttr[A, I, S] {

    override def apply(tree: Tree): Attr[A, I, S] = {
      reset()
      ann(transform(tree))
    }

    override def transform(tree: Tree): Tree =
      at(tree)(super.transform(tree))

    override def transformStats(stats: List[Tree], owner: Symbol) =
      super.transformStats(stats, owner).filter {
        case api.Empty(_) => false
        case _ => true
      }

    protected final def accTransform(tree: Tree): Tree = {
      accumulate(tree)
      complete(transformation)(tree)(tree)
    }

    @tailrec
    protected final def fixTransform(tree: Tree): Tree = {
      val recur = accTransform(tree)
      if (tree == recur) tree else fixTransform(recur)
    }
  }

  /** An abstract traversal (default is top-down break). */
  abstract class Traversal[A <: HList, I <: HList, S <: HList](
      protected val grammar: AttrGrammar[A, I, S],
      override val callback: Attr[A, I, S] =?> Unit
  ) extends Traverser with ManagedAttr[A, I, S] {

    override def apply(tree: Tree): Attr[A, I, S] = {
      reset()
      traverse(tree)
      ann(tree)
    }

    override def traverse(tree: Tree): Unit =
      at(tree)(super.traverse(tree))

    protected final def accTraverse(tree: Tree): Unit = {
      accumulate(tree)
      complete(traversal)(tree)(())
    }

    protected final def fixTraverse(tree: Tree): Unit =
      while (true) {
        accumulate(tree)
        //scalastyle:off
        traversal.applyOrElse(tree, return)
        //scalastyle:on
      }
  }

  /** A traversal / transformation factory. */
  private[ast] trait Factory {

    def traversal[A <: HList, I <: HList, S <: HList]
      (grammar: AttrGrammar[A, I, S])
      (callback: Attr[A, I, S] =?> Unit)
      : Traversal[A, I, S]

    def transform[A <: HList, I <: HList, S <: HList]
      (grammar: AttrGrammar[A, I, S])
      (template: Attr[A, I, S] =?> Tree)
      : Transform[A, I, S]
  }

  /** A traversal / transformation strategy factory. */
  private object Factory {

    /** Top-down traversal / transformation. */
    object topDown extends Factory {

      /** Top-down continue traversal. */
      override def traversal[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])
        (callback: Attr[A, I, S] =?> Unit)
        : Traversal[A, I, S]
        = Traversal.topDown(grammar)(callback)

      /** Top-down continue transformation. */
      override def transform[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])
        (template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S]
        = Transform.topDown(grammar)(template)

      /** Top-down break traversal / transformation. */
      object break extends Factory {

        /** Top-down break traversal. */
        override def traversal[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (callback: Attr[A, I, S] =?> Unit)
          : Traversal[A, I, S]
          = Traversal.topDown.break(grammar)(callback)

        /** Top-down break transformation. */
        override def transform[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (template: Attr[A, I, S] =?> Tree)
          : Transform[A, I, S]
          = Transform.topDown.break(grammar)(template)
      }

      /** Top-down exhaustive traversal / transformation. */
      object exhaust extends Factory {

        /** Top-down exhaustive traversal. */
        override def traversal[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (callback: Attr[A, I, S] =?> Unit)
          : Traversal[A, I, S]
          = Traversal.topDown.exhaust(grammar)(callback)

        /** Top-down exhaustive transformation. */
        override def transform[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (template: Attr[A, I, S] =?> Tree)
          : Transform[A, I, S]
          = Transform.topDown.exhaust(grammar)(template)
      }
    }

    /** Bottom-up traversal / transformation. */
    object bottomUp extends Factory {

      /** Bottom-up continue traversal. */
      override def traversal[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])
        (callback: Attr[A, I, S] =?> Unit)
        : Traversal[A, I, S]
        = Traversal.bottomUp(grammar)(callback)

      /** Bottom-up continue transformation. */
      override def transform[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])
        (template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S]
        = Transform.bottomUp(grammar)(template)

      /** Bottom-up break traversal / transformation. */
      object break extends Factory {

        /** Bottom-up break traversal. */
        override def traversal[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (callback: Attr[A, I, S] =?> Unit)
          : Traversal[A, I, S]
          = Traversal.bottomUp.break(grammar)(callback)

        /** Bottom-up break transformation. */
        override def transform[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (template: Attr[A, I, S] =?> Tree)
          : Transform[A, I, S]
          = Transform.bottomUp.break(grammar)(template)
      }

      /** Bottom-up exhaustive traversal / transformation. */
      object exhaust extends Factory {

        /** Bottom-up exhaustive traversal. */
        override def traversal[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (callback: Attr[A, I, S] =?> Unit)
          : Traversal[A, I, S]
          = Traversal.bottomUp.exhaust(grammar)(callback)

        /** Bottom-up exhaustive transformation. */
        override def transform[A <: HList, I <: HList, S <: HList]
          (grammar: AttrGrammar[A, I, S])
          (template: Attr[A, I, S] =?> Tree)
          : Transform[A, I, S]
          = Transform.bottomUp.exhaust(grammar)(template)
      }
    }
  }

  /** Transformations. */
  private[ast] object Transform {

    /** Top-down transformation. */
    object topDown {

      /** Top-down continue transformation. */
      def apply[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](grammar, template) {
          override final def transform(tree: Tree): Tree =
            super.transform(accTransform(tree))
        }

      /** Top-down exhaustive transformation. */
      def exhaust[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](grammar, template) {
          override final def transform(tree: Tree): Tree =
            super.transform(fixTransform(tree))
        }

      /** Top-down break transformation. */
      def break[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](grammar, template) {
          override final def transform(tree: Tree): Tree = {
            accumulate(tree)
            transformation.applyOrElse(tree, super.transform)
          }
        }
    }

    /** Bottom-up transformation. */
    object bottomUp {

      /** Bottom-up continue transformation. */
      def apply[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](grammar, template) {
          override final def transform(tree: Tree): Tree =
            accTransform(super.transform(tree))
        }

      /** Bottom-up exhaustive transformation. */
      def exhaust[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](grammar, template) {
          override final def transform(tree: Tree): Tree =
            fixTransform(super.transform(tree))
        }

      /** Bottom-up break transformation. */
      def break[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(template: Attr[A, I, S] =?> Tree)
        : Transform[A, I, S] = new Transform[A, I, S](grammar, template) {
          val matches: mutable.Set[Tree] = mutable.Set.empty

          override def reset() = {
            super.reset()
            matches.clear()
          }

          override final def transform(tree: Tree) = {
            val recur = super.transform(tree)
            val children = tree.children
            if (children.exists(matches)) {
              matches --= children
              matches += recur
              recur
            } else {
              accumulate(recur)
              if (transformation.isDefinedAt(recur)) {
                matches += recur
                transformation(recur)
              } else recur
            }
          }
        }
    }
  }

  /** Traversals. */
  private[ast] object Traversal {

    /** Top-down traversal. */
    object topDown {

      /** Top-down continue traversal. */
      def apply[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(callback: Attr[A, I, S] =?> Unit)
        : Traversal[A, I, S] = new Traversal[A, I, S](grammar, callback) {
          override final def traverse(tree: Tree): Unit = {
            accTraverse(tree)
            super.traverse(tree)
          }
        }

      /** Top-down exhaustive traversal. */
      def exhaust[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(callback: Attr[A, I, S] =?> Unit)
        : Traversal[A, I, S] = new Traversal[A, I, S](grammar, callback) {
          override final def traverse(tree: Tree): Unit = {
            fixTraverse(tree)
            super.traverse(tree)
          }
        }

      /** Top-down break traversal. */
      def break[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(callback: Attr[A, I, S] =?> Unit)
        : Traversal[A, I, S] = new Traversal[A, I, S](grammar, callback) {
          override final def traverse(tree: Tree): Unit = {
            accumulate(tree)
            traversal.applyOrElse(tree, super.traverse)
          }
        }
    }

    /** Bottom-up traversal. */
    object bottomUp {

      /** Bottom-up continue traversal. */
      def apply[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(callback: Attr[A, I, S] =?> Unit)
        : Traversal[A, I, S] = new Traversal[A, I, S](grammar, callback) {
          override final def traverse(tree: Tree): Unit = {
            super.traverse(tree)
            accTraverse(tree)
          }
        }

      /** Bottom-up exhaustive traversal. */
      def exhaust[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(callback: Attr[A, I, S] =?> Unit)
        : Traversal[A, I, S] = new Traversal[A, I, S](grammar, callback) {
          override final def traverse(tree: Tree): Unit = {
            super.traverse(tree)
            fixTraverse(tree)
          }
        }

      /** Bottom-up break traversal. */
      def break[A <: HList, I <: HList, S <: HList]
        (grammar: AttrGrammar[A, I, S])(callback: Attr[A, I, S] =?> Unit)
        : Traversal[A, I, S] = new Traversal[A, I, S](grammar, callback) {
          val matches: mutable.Set[Tree] = mutable.Set.empty

          override def reset() = {
            super.reset()
            matches.clear()
          }

          override final def traverse(tree: Tree) = {
            super.traverse(tree)
            val children = tree.children
            if (children.exists(matches)) {
              matches --= children
              matches += tree
            } else {
              accumulate(tree)
              if (traversal.isDefinedAt(tree)) {
                matches += tree
                traversal(tree)
              }
            }
          }
        }
    }
  }

  /**
   * An attributed tree.
   * @param tree The tree being attributed.
   * @param acc Accumulated attributes.
   * @param inh Inherited attributes.
   * @param syn Synthesized attributes.
   */
  case class Attr[A, I, S](tree: Tree, acc: A, inh: I, syn: S) {
    // Expose type arguments.
    type Acc = A
    type Inh = I
    type Syn = S
  }

  @implicitNotFound("could not lookup key ${K} in ${M}")
  trait Lookup[K, V, M] extends (M => V)
  object Lookup extends LookupAcc

  trait LookupAcc extends LookupInh {
    implicit def acc[K, V, A <: HList, I, S](
      implicit sel: Selector.Aux[A, K, V]
    ): Lookup[K, V, Attr[A, I, S]] = new Lookup[K, V, Attr[A, I, S]] {
      def apply(attr: Attr[A, I, S]) = sel(attr.acc)
    }
  }

  trait LookupInh extends LookupSyn {
    implicit def inh[K, V, A, I <: HList, S](
      implicit sel: Selector.Aux[I, K, V]
    ): Lookup[K, V, Attr[A, I, S]] = new Lookup[K, V, Attr[A, I, S]] {
      def apply(attr: Attr[A, I, S]) = sel(attr.inh)
    }
  }

  trait LookupSyn {
    implicit def syn[K, V, A, I, S <: HList](
      implicit sel: Selector.Aux[S, K, V]
    ): Lookup[K, V, Attr[A, I, S]] = new Lookup[K, V, Attr[A, I, S]] {
      def apply(attr: Attr[A, I, S]) = sel(attr.syn)
    }
  }

  // scalastyle:off

  /** Attribute definition. */
  class ~@[K, V](val init: FieldType[K, V])(implicit val M: Monoid[V]) {
    def apply[A <: HList](attrs: A)(
      implicit sel: Selector.Aux[A, K, V]
    ): V = sel(attrs)

    def apply[A, I, S](attr: Attr[A, I, S])(
      implicit lookup: Lookup[K, V, Attr[A, I, S]]
    ): V = lookup(attr)

    def unapply[A, I, S](attr: Attr[A, I, S])(
      implicit lookup: Lookup[K, V, Attr[A, I, S]]
    ): Option[V] = Some(lookup(attr))
  }

  /** Single attribute extraction. */
  object ~@ {
    def apply[K, V: Monoid](init: FieldType[K, V]): K ~@ V = new ~@(init)
    def unapply[A, I, S](attr: Attr[A, I, S]): Option[(u.Tree, Attr[A, I, S])] =
      Some(attr.tree, attr)
  }

  /** Multiple attributes extraction. */
  object ~@* {
    def unapply[A, I, S](attr: Attr[A, I, S]): Option[(u.Tree, Seq[Attr[A, I, S]])] =
      Some(attr.tree, Stream.continually(attr))
  }

  // scalastyle:on

  object Attr {
    private val treeOpt = Option.empty[u.Tree]
    private val bindMap = Map.empty[u.TermSymbol, u.ValDef]
    private val termUse = Map.empty[u.TermSymbol, Int].withDefaultValue(0)

    val Root       = ~@('root       ->> treeOpt)(first(None))
    val Parent     = ~@('parent     ->> treeOpt)(last(None))
    val Ancestors  = ~@('ancestors  ->> Vector.empty[u.Tree])
    val Owner      = ~@('owner      ->> api.Owner.encl)(last(api.Owner.encl))
    val OwnerChain = ~@('ownerChain ->> Vector(api.Owner.encl))
    val Defs       = ~@('defs       ->> Map.empty[u.TermSymbol, u.Tree])(overwrite)
    val BindDefs   = ~@('bindDefs   ->> bindMap)(overwrite)
    val ValDefs    = ~@('valDefs    ->> bindMap)(overwrite)
    val VarDefs    = ~@('varDefs    ->> bindMap)(overwrite)
    val ParDefs    = ~@('parDefs    ->> bindMap)(overwrite)
    val DefDefs    = ~@('defDefs    ->> Map.empty[u.MethodSymbol, u.DefDef])(overwrite)
    val Uses       = ~@('uses       ->> termUse)(merge)
    val BindUses   = ~@('bindUses   ->> termUse)(merge)
    val ValUses    = ~@('valUses    ->> termUse)(merge)
    val VarUses    = ~@('varUses    ->> termUse)(merge)
    val ParUses    = ~@('parUses    ->> termUse)(merge)
    val Assigns    = ~@('assigns    ->> termUse)(merge)
    val DefCalls   = ~@('defCalls   ->> Map.empty[u.MethodSymbol, Int].withDefaultValue(0))(merge)

    /** Constructor that discards accumulated attributes. */
    def apply[I, S](tree: Tree, inh: I, syn: S): Attr[HNil, I, S] =
      apply(tree, HNil, inh, syn)

    /** Constructor that discards accumulated and inherited attributes. */
    def apply[S](tree: Tree, syn: S): Attr[HNil, HNil, S] =
      apply(tree, HNil, HNil, syn)

    /** Collects attributes in a specified type of collection. */
    def collect[Col[x] <: Traversable[x], El](elem: Tree =?> El)
      (implicit Col: CanBuildFrom[Nothing, El, Col[El]]): Tree =?> Col[El] = {

      case tree if elem.isDefinedAt(tree) =>
        val col = Col()
        col += elem(tree)
        col.result()
    }

    /** Collects key -> value attributes in a Map. */
    def group[K, V](kv: Tree =?> (K, V)): Tree =?> Map[K, V] = {
      case tree if kv.isDefinedAt(tree) => Map(kv(tree))
    }

    /** Extractor that discards the attributes of a tree. */
    object none {
      def unapply[A, I, S](attr: Attr[A, I, S]): Option[Tree] =
        Some(attr.tree)
    }

    /** Extractor for all attributes of a tree (alias for [[Attr.unapply()]]). */
    object all {
      def unapply[A, I, S](attr: Attr[A, I, S]): Option[(Tree, A, I, S)] =
        Attr.unapply(attr)
    }

    /** Extractor for the accumulated attributes of a tree. */
    object acc {
      def unapply[A, I, S](attr: Attr[A, I, S]): Option[(Tree, A)] =
        Some(attr.tree, attr.acc)
    }

    /** Extractor for the inherited attributes of a tree. */
    object inh {
      def unapply[A, I, S](attr: Attr[A, I, S]): Option[(Tree, I)] =
        Some(attr.tree, attr.inh)
    }

    /** Extractor for the synthesized attributes of a tree. */
    object syn {
      def unapply[A, I, S](attr: Attr[A, I, S]): Option[(Tree, S)] =
        Some(attr.tree, attr.syn)
    }
  }

  /** Fluent tree traversal / transformation APIs. */
  trait TransverserAPI { this: API =>

    private val initial = {
      val nil: Attr[HNil, HNil, HNil] => HNil = const(HNil)
      AttrGrammar(HNil, HNil, HNil, nil, nil, nil)
    }

    /** Top-down traversal / transformation and attribute generation. */
    object TopDown extends Strategy[HNil, HNil, HNil](initial, Factory.topDown) {

      /** Top-down break traversal / transformation and attribute generation. */
      object break extends Strategy[HNil, HNil, HNil](initial, Factory.topDown.break)

      /** Top-down exhaustive traversal / transformation and attribute generation. */
      object exhaust extends Strategy[HNil, HNil, HNil](initial, Factory.topDown.exhaust)
    }

    /** Bottom-up traversal / transformation and attribute generation. */
    object BottomUp extends Strategy[HNil, HNil, HNil](initial, Factory.bottomUp) {

      /** Bottom-up break traversal / transformation and attribute generation. */
      object break extends Strategy[HNil, HNil, HNil](initial, Factory.bottomUp.break)

      /** Bottom-up exhaustive traversal / transformation and attribute generation. */
      object exhaust extends Strategy[HNil, HNil, HNil](initial, Factory.bottomUp.exhaust)
    }
  }
}
