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
package compiler.opt

import api.{alg => algebra}
import compiler.Common
import compiler.lang.cf.ControlFlow
import compiler.lang.comprehension.Comprehension
import compiler.lang.core.Core
import util.Graphs._

import shapeless._

import scala.collection.Map
import scala.collection.SortedMap
import scala.collection.breakOut
import scalaz.Tree

/** The fold-fusion optimization. */
private[compiler] trait FoldForestFusion extends Common {
  self: Core with Comprehension with ControlFlow =>

  /** The fold-fusion optimization. */
  private[compiler] object FoldForestFusion {

    import API._
    import Core.{Lang => core}
    import UniverseImplicits._

    type Index = Map[u.TermSymbol, u.Tree]
    type Node = (u.TermSymbol, Index)
    type Pair = (u.TermSymbol, u.Tree)

    // Folds for which the `zero`, `init` and `plus` functions don't depend on any arguments.
    private lazy val constAlgs = Map(
      DataBag.isEmpty  -> api.Sym[algebra.IsEmpty.type].asModule,
      DataBag.nonEmpty -> api.Sym[algebra.NonEmpty.type].asModule,
      DataBag.size     -> api.Sym[algebra.Size.type].asModule
    )

    // Folds for which the `zero`, `init` and `plus` functions depend on some arguments.
    private lazy val dynamicAlgs = Map(
      DataBag.count        -> api.Sym[algebra.Count.type].asModule,
      DataBag.sum          -> api.Sym[algebra.Sum.type].asModule,
      DataBag.product      -> api.Sym[algebra.Product.type].asModule,
      DataBag.exists       -> api.Sym[algebra.Exists.type].asModule,
      DataBag.forall       -> api.Sym[algebra.Forall.type].asModule,
      DataBag.find         -> api.Sym[algebra.Find.type].asModule,
      DataBag.bottom       -> api.Sym[algebra.Bottom.type].asModule,
      DataBag.top          -> api.Sym[algebra.Top.type].asModule,
      DataBag.reduceOption -> api.Sym[algebra.ReduceOpt.type].asModule
    )

    // Folds with an optional result that has to be extracted via `get`.
    private lazy val optionalAlgs = Map(
      DataBag.min -> api.Sym[algebra.Min.type].asModule,
      DataBag.max -> api.Sym[algebra.Max.type].asModule
    )

    // Folds with an optional result that has to be extracted via `get`.
    private lazy val monadOpsAlgs = Map(
      DataBag.map        -> api.Sym[algebra.Map.type].asModule,
      DataBag.flatMap    -> api.Sym[algebra.FlatMap.type].asModule,
      DataBag.withFilter -> api.Sym[algebra.WithFilter.type].asModule
    )

    private lazy val productAlgs = Map(
      //@formatter:off
      2  -> api.Sym[algebra.Alg2.type].asModule,
      3  -> api.Sym[algebra.Alg3.type].asModule,
      4  -> api.Sym[algebra.Alg4.type].asModule,
      5  -> api.Sym[algebra.Alg5.type].asModule,
      6  -> api.Sym[algebra.Alg6.type].asModule,
      7  -> api.Sym[algebra.Alg7.type].asModule,
      8  -> api.Sym[algebra.Alg8.type].asModule,
      9  -> api.Sym[algebra.Alg9.type].asModule,
      10 -> api.Sym[algebra.Alg10.type].asModule,
      11 -> api.Sym[algebra.Alg11.type].asModule,
      12 -> api.Sym[algebra.Alg12.type].asModule,
      13 -> api.Sym[algebra.Alg13.type].asModule,
      14 -> api.Sym[algebra.Alg14.type].asModule,
      15 -> api.Sym[algebra.Alg15.type].asModule,
      16 -> api.Sym[algebra.Alg16.type].asModule,
      17 -> api.Sym[algebra.Alg17.type].asModule,
      18 -> api.Sym[algebra.Alg18.type].asModule,
      19 -> api.Sym[algebra.Alg19.type].asModule,
      20 -> api.Sym[algebra.Alg20.type].asModule,
      21 -> api.Sym[algebra.Alg21.type].asModule,
      22 -> api.Sym[algebra.Alg22.type].asModule
      //@formatter:on
    )

    private lazy val cs = Comprehension.Syntax(API.DataBag.sym)

    private val ordTermSymbol = Ordering.by { (s: u.TermSymbol) =>
      val (name, i) = api.TermName.original(s.name)
      name.toString -> i
    }

    /** Constructs an index from a sequence of key-value pairs. */
    private def mkIndex(elems: (u.TermSymbol, u.Tree)*): Index =
      SortedMap(elems: _*)(ordTermSymbol)

    /** Turns a sequence of values into an index (i.e., a Map from LHS to RHS). */
    private def lhs2rhs(vals: Seq[u.ValDef]): Index =
      mkIndex(vals.collect { case core.ValDef(lhs, rhs) => lhs -> rhs }: _*)

    /** Constructs a `xs.fold(alg)`. */
    @inline private def foldAlg(xs: Option[u.Tree], B: u.Type, alg: u.TermSymbol): u.Tree =
      core.DefCall(xs, DataBag.fold1, Seq(B), Seq(Seq(core.Ref(alg))))

    /** Converts a symbol `x` to a `Some(core.Ref(x))` node. */
    @inline private def tgtOf(x: u.TermSymbol): Option[u.Ident] =
      Some(core.Ref(x))

    /** Converts a symbol `x` to a `Some(core.Ref(x))` node. */
    @inline private def tgtOf(pair: (u.TermSymbol, Any)): Option[u.Ident] =
      Some(refOf(pair))

    /** Gets the `apply` method for an object identified by a symbol `f`. */
    @inline private def appOf(f: u.TermSymbol): u.MethodSymbol =
      f.info.member(api.TermName.app).asMethod

    /** Gets the `apply` method for the `lhs` of a `lhs -> rhs` pair. */
    @inline private def appOf(pair: (u.TermSymbol, Any)): u.MethodSymbol =
      appOf(lhsOf(pair))

    /** Converts a `lhs -> rhs` pair to a `ValDef` node. */
    @inline private def defOf(pair: (u.TermSymbol, u.Tree)): u.ValDef =
      core.ValDef(pair._1, pair._2)

    /** Converts the `lhs` a `lhs -> rhs` pair to a `ValRef` node. */
    @inline private def refOf(pair: (u.TermSymbol, Any)): u.Ident =
      core.ValRef(lhsOf(pair))

    /** Convenience projection for better code readability. */
    @inline private def lhsOf[L, R](pair: (L, R)): L =
      pair._1

    /** Convenience projection for better code readability. */
    @inline private def rhsOf[L, R](pair: (L, R)): R =
      pair._2

    /** Convenience projection for better code readability. */
    @inline private def indexOf(node: Node): Index =
      node._2

    /** Creates a fresh name with prefix `alg${s}`. */
    @inline private def freshAlg(s: u.Symbol): u.TermName =
      api.TermName.fresh(s"alg$$${s.name}")

    /** Creates a fresh name with prefix `app${s}`. */
    @inline private def freshApp(s: u.Symbol): u.TermName =
      api.TermName.fresh(s"app$$${s.name}")

    /** Creates a fresh name with prefix `fun${s}`. */
    @inline private def freshFun(s: u.Symbol): u.TermName =
      api.TermName.fresh(s"fun$$${s.name}")

    /** Checks whether a let block `l` matches a trivial shape `{ s }` for a given symbol `s`. */
    @inline private def returns(l: u.Block, s: u.TermSymbol): Boolean = l match {
      case core.Let(Seq(), Seq(), core.Ref(`s`)) => true
      case _ => false
    }

    /** Creates a new product algebra type AlgX. */
    @inline
    private def productAlgOf(A: u.Type, Bs: Seq[u.Type]): u.Type = {
      val n = Bs.size
      assert(productAlgs contains n, s"Product algebra `Alg$n` is not defined")
      api.Type(productAlgs(n).companion.asClass.toTypeConstructor, A +: Bs)
    }

    /**
     * Checks whether the dependency chain between the generators is linear, that is, the
     * symbols `x$i` bound by each generator are used in `rhs$j` for all `0 < i = (j - 1) < n`.
     */
    @inline
    private def isLinear(qs: Seq[u.Tree]): Boolean = {
      val gs = qs collect { case cs.Generator(lhs, rhs) => lhs -> rhs } // collect all generators
      val ps = gs.map(lhsOf) zip gs.tail.map(rhsOf) // pair symbols with the subsequent generator rhs
      val rs = ps.forall {
        case (x$i, rhs$j) => rhs$j.exists({
          case core.Ref(`x$i`) => true // x$i used in rhs$j
          case _ => false // x$i not used in rhs$j
        })
      }
      rs
    }

    /**
     * Given an index of the vals in a block from LHS to RHS and the global control-flow graph,
     * returns a sequence of DAGs that can be fused in a single fold.
     */
    private def foldForest(
      valIndex: Index,
      cfg: FlowGraph[u.TermSymbol]
    ): Stream[Tree[u.TermSymbol]] = {
      val nst = cfg.nest.tclose

      // Initial fold forest - singleton trees of `xs` symbols in `xs.fold(...)` applications.
      var forest: Stream[Tree[u.TermSymbol]] = valIndex.collect {
        case (fold, core.DefCall(_, method, _, _))
          if DataBag.foldOps(method) => Tree.Leaf(fold)
      } (breakOut)

      // Grow the forest by extending trees with new roots until a fixed point is reached.
      var forestDidGrow = forest.nonEmpty // Marks a successful growth step.
      while (forestDidGrow) {
        forestDidGrow = false
        forest = forest.groupBy { case Tree.Node(root, _) =>
          valIndex.get(root).map({
            case cs.Comprehension(qs@(cs.Generator(_, core.Let(_, _, core.Ref(xs))) +: _), _)
              if isLinear(qs)  => xs
            case core.DefCall(Some(core.Ref(xs)), method, _, _)
              if DataBag.foldOps(method) || DataBag.monadOps(method) => xs
            case _ => root
          }).getOrElse(root)
        }.flatMap { case (target, subForest) =>
          // Sub-forest associated  with a common DefCall target.
          // => Extending the forest by prepending a new root might be possible.
          val subForestRoots = subForest.map(_.rootLabel)

          def targetIsMonadicCall = valIndex.get(target).exists({
            case cs.Comprehension(qs, _) if isLinear(qs) => true
            case core.DefCall(_, method, _, _) => DataBag.monadOps(method)
            case _ => false
          })

          val independentSubForest = for {
            tree @ Tree.Node(fold, _) <- subForest
            if (cfg.data.reachable(fold).flatMap(nst.predecessors) intersect subForestRoots).size == 1
          } yield tree

          // Target is a monadic DefCall called by exactly one sub-forest root.
          // => Prepend a root representing sequential (cata) fold-fusion.
          if (cfg.uses(target) == 1 && targetIsMonadicCall) {
            forestDidGrow = true
            Stream(Tree.Node(target, subForest))
          }

          // Target called by multiple independent sub-forest roots.
          // => Prepend a root representing parallel (banana) fold-fusion.
          else if (independentSubForest.drop(1).nonEmpty) {
            forestDidGrow = true
            // If there are more than `Max.TupleElems` folds,
            // this will generate a nested tuple in the next growing phase.
            val (fuseNow, fuseLater) = independentSubForest.splitAt(Max.TupleElems)
            val now = Tree.Node(target, fuseNow)
            if (fuseLater.isEmpty) Stream(now)
            else Stream(Tree.Node(target, now #:: fuseLater))
          }

          // Neither of the above.
          // => Extending the forest by prepending a new root is not possible.
          else subForest
        }.toStream
      }

      forest
    }

    /** Folds a Scalaz Tree bottom-up. */
    private def foldBottomUp[A, B](tree: Tree[A])(f: (A, Stream[B]) => B): Tree[B] = {
      val forest = tree.subForest.map(foldBottomUp(_)(f))
      Tree.Node(f(tree.rootLabel, forest.map(_.rootLabel)), forest)
    }

    /**
     * Collapses a tree of fold applications by performing fold-fusion rewrites in a bottom-up manner.
     *
     * Three types of rewrites are performed depending on the local structure of the tree.
     *
     * == Case 1: Inlining ==
     * Applied on leaf nodes.
     *
     * Inlines the definition of fold operators such as `min` and `max`.
     *
     * == Case 2: Banana-Fusion ==
     * Applied on intermediate nodes with multiple children.
     *
     * Fuses a sequence of folds on the same `DataBag` into a single fold,
     * provided that they are mutually independent (there are no data dependencies between them).
     *
     * Example:
     * {{{
     *   val x$1 = xs.fold(alg$1)
     *   ...
     *   val x$N = xs.fold(alg$n)
     *   // =>
     *   val (x$1, ..., x$N) = xs.fold(Alg$n(alg$1, ..., alg$n))
     * }}}
     *
     * == Case 3: Cata-Fusion ==
     * Applied on intermediate nodes with a single child (enables cata-fusion in a subsequent rewrite).
     *
     * Fuses a monadic operator `map`, `filter` and `flatMap` with a subsequent fold,
     * provided that the result the monadic operator is not referenced elsewhere.
     *
     * Examples:
     * {{{
     *   val x = xs.map(f).fold(alg)
     *   // =>
     *   val x = xs.fold(alg.Map(f, alg))
     *
     *   val x = xs.withFilter(p).fold(alg)
     *   // =>
     *   val x = xs.fold(alg.WithFilter(p, alg))
     *
     *   val x = xs.flatMap(f).fold(alg)
     *   // =>
     *   val x = xs.fold(alg.FlatMap(f, alg))
     * }}}
     */
    lazy val foldForestFusion = TreeTransform("FoldForestFusion.foldForestFusion", tree => {
      api.BottomUp.withOwner.transformWith {
        // Fuse only folds within a single block.
        case Attr.inh(let@core.Let(vals, defs, expr), owner :: _) =>
          val cfg = ControlFlow.cfg(let)
          val valIndex = lhs2rhs(vals)

          def fuse(root: u.TermSymbol, children: Stream[Node]): Node = children match {
            // Case 1: Leaves - folds to be expanded.
            case Seq() => valIndex(root) match {
              // val root = xs.fold[B](alg)
              case rhs@core.DefCall(_, DataBag.fold1, _, _) =>
                root -> mkIndex(root -> rhs)

              // Γ ⊢ xs : DataBag[A]
              // val root = xs.fold[B](zero)(init, plus)
              // =>
              // val alg$Fold$i = Fold[A, B](zero)(init, plus)
              // val root = xs.fold[B](alg$Fold$i)
              case core.DefCall(xs@Some(bag), DataBag.fold2, targs, argss) =>
                val A = api.Type.arg(1, bag.tpe)

                val alg = { // val alg$Fold$i = Fold[A, B](zero)(init, plus)
                  val mod = api.Sym[algebra.Fold.type].asModule
                  val rhs = core.DefCall(tgtOf(mod), appOf(mod), A +: targs, Seq(argss.flatten))
                  val lhs = api.ValSym(owner, freshAlg(DataBag.fold2), rhs.tpe)
                  lhs -> rhs
                }

                val fld = { // val root = xs.fold[B](alg$Fold$i)
                  val rhs = foldAlg(xs, root.info, lhsOf(alg))
                  root -> rhs
                }

                root -> mkIndex(alg, fld)

              // val root = xs.reduce[A](zero)(plus)
              // =>
              // val alg$Reduce$1 = Reduce[A](zero, plus)
              // val root = xs.fold[A](alg$Reduce$1)
              case core.DefCall(xs, DataBag.reduce, targs, argss) =>
                val alg = { // val alg$Reduce$1 = Reduce[A](zero, plus)
                  val mod = api.Sym[algebra.Reduce.type].asModule
                  val rhs = core.DefCall(tgtOf(mod), appOf(mod), targs, Seq(argss.flatten))
                  val lhs = api.ValSym(owner, freshAlg(mod), rhs.tpe)
                  lhs -> rhs
                }

                val fld = { // val root = xs.fold[A](alg$Reduce$1)
                  val rhs = foldAlg(xs, root.info, lhsOf(alg))
                  root -> rhs
                }

                root -> mkIndex(alg, fld)

              // val root = xs.isEmpty | xs.nonEmpty | xs.size
              // =>
              // val root = xs.fold(IsEmpty | NonEmpty | Size)
              case core.DefCall(xs, method, _, _)
                if constAlgs contains method =>

                val fld = { // val root = xs.fold(IsEmpty | NonEmpty | Size)
                  val rhs = foldAlg(xs, root.info, constAlgs(method))
                  root -> rhs
                }

                root -> mkIndex(fld)

              // Γ ⊢ xs : DataBag[A]
              // val root = xs.count(p) | xs.sum | xs.product
              //          | xs.exists(p) | xs.forall(p)
              //          | xs.find(p)
              //          | xs.bottom(n) | xs.top(n) | xs.reduceOption(op)
              // =>
              // val alg$<Alg>$i = <Alg>(<args>)
              // val root = xs.fold(alg$<Alg>$i)
              case core.DefCall(xs@Some(bag), method, targs, argss)
                if dynamicAlgs contains method =>
                val A = api.Type.arg(1, bag.tpe)

                val alg = { // val alg$<Alg>$i = <Alg>(<args>)
                  val mod = dynamicAlgs(method)
                  val rhs = core.DefCall(tgtOf(mod), appOf(mod), A +: targs, Seq(argss.flatten))
                  val lhs = api.ValSym(owner, freshAlg(mod), rhs.tpe)
                  lhs -> rhs
                }

                val fld = { // val root = xs.fold(alg$<Alg>$i)
                  val rhs = foldAlg(xs, root.info, lhsOf(alg))
                  root -> rhs
                }

                root -> mkIndex(alg, fld)

              // Γ ⊢ xs : DataBag[A]
              // val root = xs.min(p) | xs.max(p)
              // =>
              // val alg$<Alg>$i = alg.<Alg>[A](p)
              // val app$<Alg>$j = xs.fold[A](alg$<Alg>$i)
              // val root = app$<Alg>$j.get
              case core.DefCall(xs@Some(bag), method, _, argss)
                if optionalAlgs contains method =>
                val A = api.Type.arg(1, bag.tpe)
                val Opt = api.Type.kind1[Option](root.info)
                val mod = optionalAlgs(method)

                val alg = { // val alg$<Alg>$i = alg.<Alg>[A](p)
                  val rhs = core.DefCall(tgtOf(mod), appOf(mod), Seq(A), Seq(argss.flatten))
                  val lhs = api.ValSym(owner, freshAlg(mod), rhs.tpe)
                  lhs -> rhs
                }

                val res = { // val app$<Alg>$j = xs.fold[A](alg$<Alg>$i)
                  val lhs = api.ValSym(owner, freshApp(mod), Opt)
                  val rhs = foldAlg(xs, lhs.info, lhsOf(alg))
                  lhs -> rhs
                }

                val prj = { // val root = app$<Alg>$j.get
                  val met = Opt.member(api.TermName("get")).asTerm
                  val rhs = core.DefCall(tgtOf(res), met)
                  root -> rhs
                }

                lhsOf(res) -> mkIndex(alg, res, prj)
            }

            // Case 2: One child - fold to be cata-fused.
            case Seq((child, subIndex)) => valIndex.get(root).fold(child -> subIndex)({
              // Root denotes a comprehension of the following kind:
              //
              // val root = for {  // type DataBag[B]
              //   x <- { ...; xs } // type DataBag[A]
              //   $qs
              // } yield $ret
              //
              // val child = root.fold(alg$j)
              case cs.Comprehension(
              (q@cs.Generator(x, core.Let(Seq(), Seq(), core.Ref(xs)))) +:
                qs,
              cs.Head(ret)) if isLinear(q +: qs) =>

                val core.DefCall(/* root */ _, DataBag.fold1, _, Seq(Seq(alg$1))) = subIndex(child)

                val A = x.info
                val B = ret.tpe
                val C = child.info

                if (qs.isEmpty) {
                  // Case 2.a: exactly one generator and no guards.
                  //
                  // val fun$Map$f = (x: A) => $ret
                  // val alg$Map$f = alg.Map[A, B, C](fun$Map$f, alg$j)
                  // val child = xs.fold[C](alg$Map$f)
                  val mod = api.Sym[algebra.Map.type].asModule

                  val fun = ret match {
                    case core.Let(
                    Seq(core.ValDef(s, core.DefCall(Some(core.Ref(f)), m, Seq(), Seq(Seq(core.Ref(`x`)))))),
                    Seq(),
                    core.Ref(t)) if s == t && m.name == api.TermName.app =>
                      // $ret = { val s = f.apply(x); s }
                      // call `f` directly
                      f -> u.EmptyTree
                    case _ =>
                      // val fun$Map$f = (x: A) => $ret
                      val nme = freshFun(mod)
                      val tpe = api.Type.fun(Seq(A), B)
                      val rhs = api.Lambda(Seq(x), ret)
                      val lhs = api.ValSym(owner, nme, tpe)
                      lhs -> rhs
                  }

                  val alg = { // val alg$Map$f = alg.Map[A, B, C](fun$Map$f, alg$j)
                    val cls = mod.companion.asClass
                    val tpe = api.Type(cls.toTypeConstructor, Seq(A, B, C))
                    val rhs = core.DefCall(tgtOf(mod), appOf(mod), Seq(A, B, C), Seq(Seq(refOf(fun), alg$1)))
                    val lhs = api.ValSym(owner, freshAlg(mod), tpe)
                    lhs -> rhs
                  }

                  val fld = { // val child = xs.fold[C](alg$Map$f)
                    val rhs = foldAlg(tgtOf(xs), child.info, lhsOf(alg))
                    child -> rhs
                  }

                  child -> (subIndex ++ Seq(fun, alg, fld).filterNot(rhsOf(_).isEmpty))
                } else if (returns(ret, x) && qs.forall({ case cs.Guard(_) => true })) {
                  // Case 2.b: exactly one generator, $ret matching `{ x }`, and at least one guard
                  //
                  // val fun$WithFilter$f = (x: A) => $bdy
                  // val alg$WithFilter$j = alg.WithFilter[A, C](fun$WithFilter$f, alg$j)
                  // val child = $xs$1.fold(alg$WithFilter$j)
                  val mod = api.Sym[algebra.WithFilter.type].asModule

                  // {
                  //   val res$1 = ...
                  //   ...
                  //   val res$1 = ...
                  //   val cnj$2 = res$1 && res$2
                  //   ...
                  //   val cnj$N = cnj$(N-1) && res$N
                  //   cnj$N
                  // }
                  val bdy = {
                    val vals = qs.map({
                      case cs.Guard(core.Let(vs, Seq(), core.Ref(res))) =>
                        // predicate without control flow, simply inline vs and ex
                        res -> vs
                      case cs.Guard(p) =>
                        // predicate has control flow, wrap and call a nullary lambda
                        val fun = { // val anonfun$1 = () => p
                          val nme = api.TermName.fresh(api.TermName.lambda)
                          val tpe = api.Type.fun(Seq(), api.Type.bool)
                          val rhs = api.Lambda(body = p)
                          val lhs = api.ValSym(owner, nme, tpe)
                          lhs -> rhs
                        }
                        val res = { // val fun$1 = anonfun$1()
                          val nme = api.TermName.fresh("res")
                          val tpe = api.Type.bool
                          val rhs = api.DefCall(tgtOf(fun), appOf(fun), Seq(), Seq(Seq()))
                          val lhs = api.ValSym(owner, nme, tpe)
                          lhs -> rhs
                        }
                        lhsOf(res) -> Seq(defOf(fun), defOf(res))
                    })

                    val cnjs = if (qs.size == 1) Seq.empty[Pair] else {
                      val met = api.Type.bool.member(api.TermName("&&")).asTerm
                      val lss = vals.tail.map(_ => {
                        val nme = api.TermName.fresh("cnj")
                        val tpe = api.Type.bool
                        val lhs = api.ValSym(owner, nme, tpe)
                        lhs
                      })

                      val tgts = (lhsOf(vals.head) +: lss.tail).map(a => tgtOf(a)) // targets
                      val args = vals.tail.map(a => refOf(a)) // args

                      (lss zip (tgts zip args)).map({ case (lhs, (tgt, arg)) =>
                        val rhs = core.DefCall(tgt, met, Seq(), Seq(Seq(arg)))
                        lhs -> rhs
                      })
                    }

                    val expr =
                      if (qs.size == 1) core.Ref(lhsOf(vals.head))
                      else core.Ref(lhsOf(cnjs.last))

                    core.Let(vals.flatMap(rhsOf), Seq(), expr)
                  }

                  val fun = bdy match {
                    case core.Let(
                    Seq(core.ValDef(s, core.DefCall(Some(core.Ref(f)), m, Seq(), Seq(Seq(core.Ref(`x`)))))),
                    Seq(),
                    core.Ref(t)) if s == t && m.name == api.TermName.app =>
                      // $rhs = { val s = f.apply(x); s }
                      // call `f` directly
                      f -> u.EmptyTree
                    case _ =>
                      // val fun$WithFilter$f = (x: A) => { ... }
                      val nme = freshFun(mod)
                      val tpe = api.Type.fun(Seq(A), api.Type.bool)
                      val rhs = api.Lambda(Seq(x), bdy)
                      val lhs = api.ValSym(owner, nme, tpe)
                      lhs -> rhs
                  }

                  val alg = { // val alg$WithFilter$j = alg.WithFilter[A, C](fun$WithFilter$f, alg$j)
                    val cls = mod.companion.asClass
                    val tpe = api.Type(cls.toTypeConstructor, Seq(A, C))
                    val rhs = core.DefCall(tgtOf(mod), appOf(mod), Seq(A, C), Seq(Seq(core.Ref(lhsOf(fun)), alg$1)))
                    val lhs = api.ValSym(owner, freshAlg(mod), tpe)
                    lhs -> rhs
                  }

                  val fld = { // val child = xs.fold[C](alg$Map$f)
                    val rhs = foldAlg(tgtOf(xs), child.info, lhsOf(alg))
                    child -> rhs
                  }

                  child -> (subIndex ++ Seq(fun, alg, fld).filterNot(rhsOf(_).isEmpty))
                } else {
                  // Case 2.c: multiple generators (and zero or more guards)
                  //
                  // val fun$FlatMap$f = (x: A) => {
                  //   val xs$f = for {
                  //     $qs
                  //   } yield $ret
                  //   xs$f
                  // }
                  // val alg$FlatMap$f = alg.FlatMap[A, B, C](fun$FlatMap$f, alg$j)
                  // val child = xs$1.fold(alg$FlatMap$f)
                  val mod = api.Sym[algebra.FlatMap.type].asModule

                  val fun = { // val fun$Map$f = (x: A) => $ret
                    val nme = freshFun(mod)
                    val bdy = {
                      val (pre, qs1, ret1) = qs.head match {
                        case cs.Generator(_, _) =>
                          (Seq.empty[u.ValDef], qs, ret)
                        case _ =>
                          val ss1 = {
                            val nme = api.TermName.fresh("ss")
                            val seq = api.Sym[Seq.type].asModule
                            val tgt = Some(core.Ref(seq))
                            val met = seq.info.member(api.TermName.app).asMethod
                            val rhs = api.DefCall(tgt, met, Seq(A), Seq(Seq(core.Ref(x))))
                            val lhs = api.ValSym(owner, nme, rhs.tpe)
                            lhs -> rhs
                          }
                          val xs1 = {
                            val nme = api.TermName.fresh("xs")
                            val tgt = Some(api.Ref(API.DataBag$.sym))
                            val met = API.DataBag$.apply
                            val rhs = api.DefCall(tgt, met, Seq(A), Seq(Seq(refOf(ss1))))
                            val lhs = api.ValSym(owner, nme, rhs.tpe)
                            lhs -> rhs
                          }
                          val lhs = api.TermSym.free(x)
                          val rhs = core.Let(expr = refOf(xs1))
                          val sub = api.Sym.subst(owner, Seq(x -> lhs))
                          val qs1 = cs.Generator(lhs, rhs) +: qs.map(sub)
                          val ret1 = sub(ret).asInstanceOf[u.Block]
                          (Seq(defOf(ss1), defOf(xs1)), qs1, ret1)
                      }
                      val nme = api.TermName.fresh("ys")
                      val rhs = cs.Comprehension(qs1, cs.Head(ret1))
                      val lhs = api.ValSym(owner, nme, root.info)
                      core.Let(pre ++ Seq(core.ValDef(lhs, rhs)), Seq(), core.Ref(lhs))
                    }
                    val rhs = api.Lambda(Seq(x), bdy)
                    val tpe = api.Type.fun(Seq(A), root.info)
                    val lhs = api.ValSym(owner, nme, tpe)
                    lhs -> rhs
                  }

                  val alg = { // val alg$Map$f = alg.Map[A, B, C](fun$Map$f, alg$j)
                    val cls = mod.companion.asClass
                    val tpe = api.Type(cls.toTypeConstructor, Seq(A, B, C))
                    val rhs = core.DefCall(tgtOf(mod), appOf(mod), Seq(A, B, C), Seq(Seq(refOf(fun), alg$1)))
                    val lhs = api.ValSym(owner, freshAlg(mod), tpe)
                    lhs -> rhs
                  }

                  val fld = { // val child = xs.fold[C](alg$Map$f)
                    val rhs = foldAlg(tgtOf(xs), child.info, lhsOf(alg))
                    child -> rhs
                  }

                  child -> (subIndex ++ Seq(fun, alg, fld))
                }

              // Root is a monad op (Emma LNF)
              case core.DefCall(xs@Some(bag), method, _, Seq(Seq(f), _*))
                if monadOpsAlgs contains method =>
                val core.DefCall(/* root */ _, DataBag.fold1, targs, Seq(Seq(alg$1))) = subIndex(child)

                if (method != DataBag.withFilter) {
                  // Γ ⊢ xs : DataBag[A]
                  // val root = xs.<map|flatMap>[B](f)
                  // val alg$i = <Alg>(...)
                  // val child = root.fold[C](alg$i)
                  // =>
                  // val alg$i = <Alg>(...)
                  // val alg$<Map|FlatMap>$j = alg.<Map|FlatMap>[A,B,C](f, alg$<Map|FlatMap>$j)
                  // val child = xs.fold[C](alg$<Map|FlatMap>$j)
                  val A = api.Type.arg(1, bag.tpe)
                  val B = api.Type.arg(1, root.info)
                  val C = targs.head

                  val alg = { // val alg$<Map|FlatMap>$j = alg.<Map|FlatMap>[A,B,C](f, alg$<Map|FlatMap>$j)
                    val mod = monadOpsAlgs(method)
                    val cls = mod.companion.asClass
                    val tpe = api.Type(cls.toTypeConstructor, Seq(A, B, C))
                    val rhs = core.DefCall(tgtOf(mod), appOf(mod), Seq(A, B, C), Seq(Seq(f, alg$1)))
                    val lhs = api.ValSym(owner, freshAlg(mod), tpe)
                    lhs -> rhs
                  }

                  val fld = { // val child = xs.fold[C](alg$<Map|FlatMap>$j)
                    val rhs = foldAlg(xs, child.info, lhsOf(alg))
                    child -> rhs
                  }

                  child -> (subIndex ++ Seq(alg, fld))
                } else {
                  // Γ ⊢ xs : DataBag[A]
                  // val root = xs.withFilter(p)
                  // val alg$i = <Alg>(...)
                  // val child = root.fold[B](alg$1)
                  // =>
                  // val alg$i = <Alg>(...)
                  // val alg$WithFilter$j = alg.WithFilter(p, alg$i)
                  // val child = xs.fold[B](alg$WithFilter$j)
                  val A = api.Type.arg(1, bag.tpe)
                  val B = targs.head

                  val alg = { // val alg$WithFilter$j = alg.WithFilter(p, alg$i)
                    val mod = monadOpsAlgs(method)
                    val cls = mod.companion.asClass
                    val tpe = api.Type(cls.toTypeConstructor, Seq(A, B))
                    val rhs = core.DefCall(tgtOf(mod), appOf(mod), Seq(A, B), Seq(Seq(f, alg$1)))
                    val lhs = api.ValSym(owner, freshAlg(mod), tpe)
                    lhs -> rhs
                  }

                  val fld = { // val child = xs.fold[B](alg$WithFilter$j)
                    val rhs = foldAlg(xs, child.info, lhsOf(alg))
                    child -> rhs
                  }

                  child -> (subIndex ++ Seq(alg, fld))
                }

              case _ =>
                // root is not a monadic op or a comprehension
                child -> subIndex
            })

            // Case 3: Many children - folds to be banana-fused.
            //
            // val x$1 = xs.fold(alg$1)
            // ...
            // val x$N = xs.fold(alg$N)
            // =>
            // val alg$AlgN$i = AlgN[A, (B$1, ..., B$n)](alg$1, ..., alg$N)
            // val app$AlgN$j = xs.fold(alg$AlgN$i)
            // val x$1 = app$AlgN$j._1
            // ...
            // val x$N = app$AlgN$j._N
            case _ =>
              val cs = children.sortBy(_._1)(ordTermSymbol) // order the children sequence
            val n = cs.size

              val mod = productAlgs(n)
              val folds = for ((child, subIndex) <- cs) yield subIndex(child)

              val A = api.Type.arg(1, root.info)
              val B = api.Type.tupleOf(for ((child, _) <- cs) yield child.info)
              val Bs = for ((child, _) <- cs) yield child.info

              val alg = { // val alg$AlgN$i = AlgN[A, (B$1, ..., B$n)](alg$1, ..., alg$n)
                val tpe = productAlgOf(A, Bs)
                val ags = for {
                  core.DefCall(_, DataBag.fold1, _, Seq(Seq(alg))) <- folds
                } yield alg
                val rhs = core.DefCall(tgtOf(mod), appOf(mod), A +: Bs, Seq(ags))
                val lhs = api.ValSym(owner, freshAlg(mod), tpe)
                lhs -> rhs
              }

              val res = { // val app$AlgN$j = xs.fold(alg$AlgN$i)
                val ars = Seq(refOf(alg))
                val rhs = core.DefCall(tgtOf(root), DataBag.fold1, Seq(B), Seq(ars))
                val lhs = api.ValSym(owner, freshApp(mod), B)
                lhs -> rhs
              }

              val pjs = for { // val x$i = app$AlgN$j._i
                ((child, _), i) <- cs.zipWithIndex
                _i = lhsOf(res).info.member(api.TermName(s"_${i + 1}")).asTerm
              } yield child -> core.DefCall(tgtOf(res), _i)

              // update the index
              // `pjs` overrides existing keys, while
              // `alg` and `res` introduce new entries
              val oldIndex = cs.map(indexOf).reduce(_ ++ _)
              val newIndex = oldIndex ++ pjs ++ Seq(alg, res)

              // call recusively to handle enabled cata-fusion
              fuse(root, Stream(lhsOf(res) -> newIndex))
          }

          // build the original fold forest
          val forest = foldForest(valIndex, cfg)

          // Fuse each tree in the forest. Each tree node holds
          // - an index of the current values rooted at that node, and
          // - the symbol of the fold represented by that node.
          // Finally, merge the indexes of the resulting singleton trees.
          val fusedIndex = forest.map(tree =>
            rhsOf(foldBottomUp[u.TermSymbol, Node](tree)(fuse).rootLabel)
          ).fold(Map.empty)(_ ++ _)

          // collect monad ops nodes which were part of the fused tree
          val fusedMonadOps = forest.map(tree =>
            tree.foldRight(Set.empty[u.TermSymbol])((s, ops) => valIndex.get(s) match {
              case Some(core.DefCall(_, m, _, _)) if DataBag.monadOps(m) => ops + s
              case Some(cs.Comprehension(qs, _)) if isLinear(qs) => ops + s
              case _ => ops
            })
          ).fold(Set.empty)(_ ++ _)

          // Build a new data-flow graph to determine the topological order of vals.
          if (fusedIndex.isEmpty) let else {
            val index = valIndex ++ fusedIndex -- fusedMonadOps
            val dataFlow0 = index.mapValues(_.collect {
              case core.Ref(x) if index.contains(x) => x
            }.toSet)

            // We add edges from all non-implicit ValDef to all implicit ValDefs.
            // This is necessary, because implicits are not resolved at this point in the pipeline,
            // so we don't see the real dependencies from just looking at the refs.
            // Note that unfortunately this can still go wrong in certain cases:
            //  - implicit dependencies between implicit ValDefs
            //  - an implicit ValDef has a dependency on a non-implicit ValDef
            // But it works for common cases, e.g., with addContext.
            val implicitValSyms = vals.filter(_.symbol.isImplicit).map(_.symbol.asTerm)
            val dataFlow =
              if (implicitValSyms.isEmpty) {
                dataFlow0
              } else {
                dataFlow0.map { case (vdsym, edges) =>
                  if (vdsym.isImplicit) {
                    (vdsym, edges)
                  } else {
                    (vdsym, edges ++ implicitValSyms.toSet)
                  }
                }
              }

            val sortedVals = for {
              lhs <- topoSort(dataFlow).get
            } yield core.ValDef(lhs, index(lhs))
            core.Let(sortedVals, defs, expr)
          }
      }._tree(tree)
    })
  }
}
