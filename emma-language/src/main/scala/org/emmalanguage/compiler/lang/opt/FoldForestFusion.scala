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
package compiler.lang.opt

import api.{alg => algebra}
import compiler.Common
import compiler.lang.comprehension.Comprehension
import compiler.lang.cf.ControlFlow
import compiler.lang.core.Core
import util.Graphs._

import shapeless._

import scala.collection.Map
import scala.collection.SortedMap
import scala.collection.breakOut
import scalaz.Tree

/** The fold-fusion optimization. */
private[compiler] trait FoldForestFusion extends Common {
  this: Core with ControlFlow with Comprehension =>

  /** The fold-fusion optimization. */
  object FoldFusion {

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

    private val ordTermSymbol = Ordering.by((s: u.TermSymbol) => s.name.toString)

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

    /** Creates a new product algebra type AlgX. */
    @inline
    private def productAlgOf(A: u.Type, Bs: Seq[u.Type]): u.Type = {
      val n = Bs.size
      assert(productAlgs contains n, s"Product algebra `Alg$n` is not defined")
      api.Type(productAlgs(n).companion.asClass.toTypeConstructor, A +: Bs)
    }

    /**
     * Given an index of the vals in a block from LHS to RHS and the global control-flow graph,
     * returns a sequence of DAGs that can be fused in a single fold.
     */
    private def foldForest(
      valIndex: Index,
      cfg: CFG.FlowGraph[u.TermSymbol]
    ): Stream[Tree[u.TermSymbol]] = {
      val CFG.FlowGraph(refCount, _, _, dataFlow) = cfg

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
            case core.DefCall(Some(core.Ref(xs)), method, _, _)
              if DataBag.foldOps(method) || DataBag.monadOps(method) => xs
            case _ => root
          }).getOrElse(root)
        }.flatMap { case (target, subForest) =>
          // Sub-forest associated  with a common DefCall target.
          // => Extending the forest by prepending a new root might be possible.
          val subForestRoots = subForest.map(_.rootLabel)

          def targetIsMonadicCall = valIndex.get(target).exists({
            case core.DefCall(_, method, _, _) => DataBag.monadOps(method)
            case _ => false
          })

          val independentSubForest = for {
            tree @ Tree.Node(fold, _) <- subForest
            if (dataFlow.reachable(fold) intersect subForestRoots).size == 1
          } yield tree

          // Target is a monadic DefCall called by exactly one sub-forest root.
          // => Prepend a root representing sequential (cata) fold-fusion.
          if (refCount(target) == 1 && targetIsMonadicCall) {
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

      forest.filter(_.subForest.nonEmpty)
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
    def foldForestFusion(cfg: CFG.FlowGraph[u.TermSymbol]): u.Tree => u.Tree =
      api.BottomUp.withOwner.transformWith {
        // Fuse only folds within a single block.
        case Attr.inh(let @ core.Let(vals, defs, expr), owner :: _) =>
          val valIndex = lhs2rhs(vals)

          def fuse(root: u.TermSymbol, children: Stream[Node]): Node = children match {
            // Case 1: Leaves - folds to be expanded.
            case Seq() => valIndex(root) match {
              // val root = xs.fold[B](alg)
              case rhs @ core.DefCall(_, DataBag.fold1, _, _) =>
                root -> mkIndex(root -> rhs)

              // Γ ⊢ xs : DataBag[A]
              // val root = xs.fold[B](zero)(init, plus)
              // =>
              // val alg$Fold$i = Fold[A, B](zero)(init, plus)
              // val root = xs.fold[B](alg$Fold$i)
              case core.DefCall(xs @ Some(bag), DataBag.fold2, targs, argss) =>
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
              case core.DefCall(xs @ Some(bag), method, targs, argss)
                if dynamicAlgs contains method =>
                val A   = api.Type.arg(1, bag.tpe)

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
              case core.DefCall(xs @ Some(bag), method, _, argss)
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

              // Γ ⊢ xs : DataBag[A]
              // val root = xs.sample(n)
              // =>
              // val alg$Sample$i = alg.Sample(n)
              // val app$Sample$j = xs.fold[A](alg$Sample$i)
              // val root = app$Sample$j._2
              case core.DefCall(xs @ Some(bag), DataBag.sample, _, argss) =>
                val A = api.Type.arg(1, bag.tpe)
                val Pair = api.Type.tupleOf(Seq(api.Type.long, root.info))

                val mod = api.Sym[algebra.Sample.type].asModule

                val alg = { // val alg$Sample$i = alg.Sample(n)
                  val rhs = core.DefCall(tgtOf(mod), appOf(mod), Seq(A), argss)
                  val lhs = api.ValSym(owner, freshAlg(mod), rhs.tpe)
                  lhs -> rhs
                }

                val res = { // val app$Sample$j = xs.fold[A](alg$Sample$i)
                  val lhs = api.ValSym(owner, freshApp(mod), Pair)
                  val rhs = foldAlg(xs, lhs.info, lhsOf(alg))
                  lhs -> rhs
                }

                val prj = { // val root = app$Sample$j._2
                  val met = Pair.member(api.TermName("_2")).asTerm
                  val rhs = core.DefCall(tgtOf(res), met)
                  root -> rhs
                }

                lhsOf(res) -> mkIndex(alg, res, prj)
            }

            // Case 2: One child - fold to be cata-fused.
            case Seq((child, subIndex)) => valIndex.get(root).fold(child -> subIndex)({
              case core.DefCall(xs @ Some(bag), method, _, Seq(Seq(f), _*))
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

              val res  = { // val app$AlgN$j = xs.fold(alg$AlgN$i)
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

          // Fuse each tree in the forest. Each tree node holds
          // - an index of the current values rooted at that node, and
          // - the symbol of the fold represented by that node.
          // Finally, merge the indexes of the resulting singleton trees.
          val fusedIndex = foldForest(valIndex, cfg).map(tree =>
            rhsOf(foldBottomUp[u.TermSymbol, Node](tree)(fuse).rootLabel)
          ).fold(Map.empty)(_ ++ _)

          // Build a new data-flow graph to determine the topological order of vals.
          if (fusedIndex.isEmpty) let else {
            val index = valIndex ++ fusedIndex
            val dataFlow = index.mapValues(_.collect {
              case core.Ref(x) if index.contains(x) => x
            }.toSet)
            val sortedVals = for {
              lhs <- topoSort(dataFlow).get
            } yield core.ValDef(lhs, index(lhs))
            core.Let(sortedVals, defs, expr)
          }
      }._tree
  }
}
