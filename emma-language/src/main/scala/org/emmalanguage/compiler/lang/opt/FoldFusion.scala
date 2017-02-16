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

import compiler.Common
import compiler.ir.Aggregations
import compiler.lang.cf.ControlFlow
import compiler.lang.core.Core
import util.Functions
import util.Graphs._

import scalaz.Tree
import shapeless._

import scala.collection.breakOut

/** The fold-fusion optimization. */
private[compiler] trait FoldFusion extends Common {
  this: Core with ControlFlow =>

  /** The fold-fusion optimization. */
  object FoldFusion {

    import UniverseImplicits._
    import Core.{Lang => core}

    // Function utils.
    private val functions   = core.Ref(api.Sym[Functions.type].asModule)
    private val conditional = functions.tpe.member(api.TermName("conditional")).asMethod
    private val flatFold    = functions.tpe.member(api.TermName("flatFold")).asMethod

    // Folds for which the `emp`, `sng` and `uni` functions don't depend on any arguments.
    private val constFolds = Map(
      API.isEmpty  -> api.Sym[Aggregations.IsEmpty.type],
      API.nonEmpty -> api.Sym[Aggregations.NonEmpty.type],
      API.size     -> api.Sym[Aggregations.Size.type]
    ).mapValues(_.asModule)

    // Folds for which the `emp`, `sng` and `uni` functions depend on some arguments.
    private val dynamicFolds = Map(
      API.count        -> api.Sym[Aggregations.Count.type],
      API.sum          -> api.Sym[Aggregations.Sum.type],
      API.product      -> api.Sym[Aggregations.Product.type],
      API.exists       -> api.Sym[Aggregations.Exists.type],
      API.forall       -> api.Sym[Aggregations.Forall.type],
      API.find         -> api.Sym[Aggregations.Find.type],
      API.bottom       -> api.Sym[Aggregations.Bottom.type],
      API.top          -> api.Sym[Aggregations.Top.type],
      API.reduceOption -> api.Sym[Aggregations.ReduceOpt.type]
    ).mapValues(_.asModule)

    // Folds with an optional result that has to be extracted via `get`.
    private val optionalFolds = Map(
      API.min -> api.Sym[Aggregations.Min.type],
      API.max -> api.Sym[Aggregations.Max.type]
    ).mapValues(_.asModule)

    /**
     * Given an index of the vals in a block from LHS to RHS and the global control-flow graph,
     * returns a sequence of DAGs that can be fused in a single fold.
     */
    private def foldForest(
      valIndex: Map[u.TermSymbol, u.Tree],
      cfg: CFG.FlowGraph[u.TermSymbol]
    ): Stream[Tree[u.TermSymbol]] = {
      val CFG.FlowGraph(refCount, _, _, dataFlow) = cfg

      // Initial fold forest - singleton trees of `xs` symbols in `xs.fold(...)` applications.
      var forest: Stream[Tree[u.TermSymbol]] = valIndex.collect {
        case (fold, core.DefCall(_, method, _, _))
          if API.foldOps(method) => Tree.leaf(fold)
      } (breakOut)

      // Grow the forest by extending trees with new roots until a fixed point is reached.
      var forestDidGrow = true // Marks a successful growth step.
      while (forestDidGrow) {
        forestDidGrow = false
        forest = forest.groupBy { case Tree.Node(root, _) =>
          (for { // Group by root DataBag reference.
            core.DefCall(Some(core.Ref(xs)), method, _, _) <- valIndex.get(root)
            if API.foldOps(method) || API.monadOps(method)
          } yield xs).getOrElse(root)
        }.flatMap { case (target, subForest) =>
          // Sub-forest associated with a common DefCall target.
          // => Extending the forest by prepending a new root might be possible.
          val subForestRoots = subForest.map(_.rootLabel)

          def targetIsMonadicCall = (for {
            core.DefCall(_, method, _, _) <- valIndex.get(target)
          } yield API.monadOps(method)).getOrElse(false)

          val independentSubForest = for {
            tree @ Tree.Node(fold, _) <- subForest
            if (dataFlow.reachable(fold) intersect subForestRoots).size == 1
          } yield tree

          // Target is a monadic DefCall called by exactly one sub-forest root.
          // => Prepend a root representing sequential (cata) fold-fusion.
          if (refCount(target) == 1 && targetIsMonadicCall) {
            forestDidGrow = true
            Stream(Tree.node(target, subForest))
          }

          // Target called by multiple independent sub-forest roots.
          // => Prepend a root representing parallel (banana) fold-fusion.
          else if (independentSubForest.drop(1).nonEmpty) {
            forestDidGrow = true
            // If there are more than `Max.TupleElems` folds,
            // this will generate a nested tuple in the next growing phase.
            val (fuseNow, fuseLater) = independentSubForest.splitAt(Max.TupleElems)
            val now = Tree.node(target, fuseNow)
            if (fuseLater.isEmpty) Stream(now)
            else Stream(Tree.node(target, now #:: fuseLater))
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
      Tree.node(f(tree.rootLabel, forest.map(_.rootLabel)), forest)
    }

    /** Turns a sequence of values into a Map from LHS to RHS. */
    private def lhs2rhs(vals: Seq[u.ValDef]): Map[u.TermSymbol, u.Tree] =
      vals.collect { case core.ValDef(lhs, rhs) => lhs -> rhs } (breakOut)

    /** Returns the values necessary for an aggregation. */
    private def aggregation(xs: Option[u.Tree], fold: u.TermSymbol, agg: u.TermSymbol) = {
      val owner   = fold.owner
      val target  = Some(core.Ref(agg))
      val lambdas = for {
        name <- Seq("emp", "sng", "uni")
        rhs = core.DefCall(target, agg.info.member(api.TermName(name)).asTerm)
        lhs = api.ValSym(owner, api.TermName.fresh(name), rhs.tpe)
      } yield lhs -> rhs
      val Seq(emp, sng, uni) = for ((f, _) <- lambdas) yield core.Ref(f)
      val targs = Seq(emp.tpe)
      val argss = Seq(Seq(emp), Seq(sng, uni))
      lambdas.toMap + (fold -> core.DefCall(xs, API.fold, targs, argss))
    }

    /**
     * Performs the fold-fusion optimization on `DataBag` expressions.
     * There are two optimizations performed by this transformation:
     *
     * == Banana-fusion ==
     * A sequence of folds on the same `DataBag` can be fused into a single fold,
     * provided that they are independent (there are no data dependencies between them).
     *
     * Example:
     * {{{
     *   val f$1 = xs.fold(emp$1)(sng$1, uni$1)
     *   ...
     *   val f$n = xs.fold(emp$n)(sng$n, uni$n)
     *   // =>
     *   val (f$1, ..., f$n) = xs.fold((emp$1, ..., emp$n))(
     *     x => (sng$1(x), ..., sng$n(x)),
     *     (x, y) => (uni$1(x._1, y._1), ..., uni$n(x._n, y._n)))
     * }}}
     *
     * == Cata-fusion ==
     * The monadic operations `map`, `filter` and `flatMap` can be fused with a subsequent fold,
     * provided that the result of such operations is not referenced elsewhere.
     *
     * Examples:
     * {{{
     *   val f$1 = xs.map(f).fold(emp$1)(sng$1, uni$1)
     *   // =>
     *   val f$1 = xs.fold(emp$1)(f.andThen(sng$1), uni$1)
     *
     *   val f$2 = xs.filter(p).fold(emp$2)(sng$2, uni$2)
     *   // =>
     *   val f$2 = xs.fold(emp$2)(x => if (p(x)) sng$2(x) else emp$2, uni$2)
     *
     *   val f$3 = xs.flatMap(g).fold(emp$3)(sng$3, uni$3)
     *   // =>
     *   val f$3 = xs.fold(emp$3)(g.andThen(_.fold(emp$3)(sng$3, uni$3)), uni$3)
     * }}}
     */
    def foldForestFusion(cfg: CFG.FlowGraph[u.TermSymbol]): u.Tree => u.Tree =
      api.BottomUp.withOwner.transformWith {
        // Fuse only folds within a single block.
        case Attr.inh(let @ core.Let(vals, defs, expr), owner :: _) =>
          val valIndex = lhs2rhs(vals)
          // Fuse each DAG in the forest.
          val fusedIndex = foldForest(valIndex, cfg).map { tree =>
            // Each node holds an index of the current values rooted at that node
            // and the symbol of the fold represented by that node.
            foldBottomUp[u.TermSymbol, (u.TermSymbol, Map[u.TermSymbol, u.Tree])](tree) {
              // Leaves - folds to be expanded.
              case (root, Seq()) => valIndex(root) match {
                // xs.fold(emp)(sng, uni)
                case rhs @ core.DefCall(_, API.fold, _, _) =>
                  root -> Map(root -> rhs)

                // xs.reduce(emp)(uni)
                // =>
                // val reduce = Reduce(emp, uni)
                // xs.fold(reduce.emp)(reduce.sng, reduce.uni)
                case core.DefCall(xs, API.reduce, targs, argss) =>
                  val module = core.Ref(api.Sym[Aggregations.Reduce.type].asModule)
                  val apply  = module.tpe.member(api.TermName.app).asMethod
                  val rhs    = core.DefCall(Some(module), apply, targs, argss.take(2))
                  val reduce = api.ValSym(owner, api.TermName.fresh(API.reduce), rhs.tpe)
                  root -> (aggregation(xs, root, reduce) + (reduce -> rhs))

                // xs.isEmpty | xs.nonEmpty | xs.size
                // =>
                // xs.fold(<Agg>.emp)(<Agg>.sng, <Agg>.uni)
                case core.DefCall(xs, method, _, _)
                  if constFolds.contains(method) =>
                  root -> aggregation(xs, root, constFolds(method))

                // xs.count(p) | xs.sum | xs.product | xs.exists(p) | xs.forall(p) | xs.find(p) |
                // xs.bottom(n) | xs.top(n) | xs.reduceOption(op)
                // =>
                // val agg = <Agg>(..<args>)
                // xs.fold(agg.emp)(agg.sng, agg.uni)
                case core.DefCall(xs @ Some(bag), method, targs, argss)
                  if dynamicFolds.contains(method) =>
                  val module = core.Ref(dynamicFolds(method))
                  val apply  = module.tpe.member(api.TermName.app).asMethod
                  val Elem   = api.Type.arg(1, bag.tpe)
                  val rhs    = core.DefCall(Some(module), apply, Elem +: targs, Seq(argss.flatten))
                  val agg    = api.ValSym(owner, api.TermName.fresh(method), rhs.tpe)
                  root -> (aggregation(xs, root, agg) + (agg -> rhs))

                // xs.min | xs.max
                // =>
                // val agg = <Agg>(..<args>)
                // val tmp = xs.fold(agg.emp)(agg.sng, agg.uni)
                // tmp.get
                case core.DefCall(xs @ Some(bag), method, _, argss)
                  if optionalFolds.contains(method) =>
                  val module  = core.Ref(optionalFolds(method))
                  val apply   = module.tpe.member(api.TermName.app).asTerm
                  val Elem    = api.Type.arg(1, bag.tpe)
                  val rhs     = core.DefCall(Some(module), apply, Seq(Elem), Seq(argss.flatten))
                  val lhs     = api.ValSym(owner, api.TermName.fresh(method), rhs.tpe)
                  val Opt     = api.Type.kind1[Option](root.info)
                  val tmp     = api.ValSym(owner, api.TermName.fresh(root), Opt)
                  val get     = Opt.member(api.TermName("get")).asTerm
                  val extract = core.DefCall(Some(core.Ref(tmp)), get)
                  tmp -> (aggregation(xs, tmp, lhs) ++ Seq(lhs -> rhs, root -> extract))

                // xs.sample(n)
                // =>
                // val sample = Sample(n)
                // val tmp = xs.fold(sample.emp)(sample.sng, sample.uni)
                // tmp._2
                case core.DefCall(xs @ Some(bag), API.sample, _, argss) =>
                  val module  = core.Ref(api.Sym[Aggregations.Sample.type].asModule)
                  val apply   = module.tpe.member(api.TermName.app).asTerm
                  val Elem    = api.Type.arg(1, bag.tpe)
                  val rhs     = core.DefCall(Some(module), apply, Seq(Elem), argss)
                  val sample  = api.ValSym(owner, api.TermName.fresh(API.sample), rhs.tpe)
                  val Pair    = api.Type.tupleOf(Seq(api.Type.long, root.info))
                  val tmp     = api.ValSym(owner, api.TermName.fresh(root), Pair)
                  val _2      = Pair.member(api.TermName("_2")).asTerm
                  val extract = core.DefCall(Some(core.Ref(tmp)), _2)
                  tmp -> (aggregation(xs, tmp, sample) ++ Seq(sample -> rhs, root -> extract))
              }

              // One child - fold to be cata-fused.
              case (root, Seq((child, subIndex))) => valIndex(root) match {
                // val xs = ys.map(f)
                // ys.fold(emp)(sng, uni)
                // =>
                // val sng$f = f.andThen(sng)
                // ys.fold(emp)(sng$f, uni)
                case core.DefCall(xs, API.map, _, Seq(Seq(f), _*)) =>
                  val core.DefCall(_, _, b, Seq(Seq(emp), Seq(sng, uni))) = subIndex(child)
                  val andThen = f.tpe.member(api.TermName("andThen")).asTerm
                  val rhs     = core.DefCall(Some(f), andThen, b, Seq(Seq(sng)))
                  val lhs     = api.ValSym(owner, api.TermName.fresh("sng"), rhs.tpe)
                  val lambdas = Seq(Seq(emp), Seq(core.Ref(lhs), uni))
                  val cata    = core.DefCall(xs, API.fold, b, lambdas)
                  child -> (subIndex ++ Seq(lhs -> rhs, child -> cata))

                // val xs = ys.filter(p)
                // xs.fold[b](emp)(sng, uni)
                // =>
                // val sng$p = Functions.conditional(sng, p, emp)
                // ys.fold[b](emp)(sng$p, uni)
                case core.DefCall(xs, API.withFilter, _, Seq(Seq(p))) =>
                  val core.DefCall(_, _, b, Seq(Seq(emp), Seq(sng, uni))) = subIndex(child)
                  val targs   = sng.tpe.dealias.widen.typeArgs
                  val argss   = Seq(Seq(sng, p, emp))
                  val rhs     = core.DefCall(Some(functions), conditional, targs, argss)
                  val lhs     = api.ValSym(owner, api.TermName.fresh("sng"), rhs.tpe)
                  val lambdas = Seq(Seq(emp), Seq(core.Ref(lhs), uni))
                  val cata    = core.DefCall(xs, API.fold, b, lambdas)
                  child -> (subIndex ++ Seq(lhs -> rhs, child -> cata))

                // val xs = ys.flatMap(f)
                // xs.fold[b](emp)(sng, uni)
                // =>
                // val sng$f = Functions.flatFold(f, emp, sng, uni)
                // ys.fold[b](emp)(sng$f, uni)
                case core.DefCall(xs, API.flatMap, _, Seq(Seq(f), _*)) =>
                  val core.DefCall(_, _, b, Seq(Seq(emp), Seq(sng, uni))) = subIndex(child)
                  val targs   = api.Type.arg(1, f.tpe) :: sng.tpe.dealias.widen.typeArgs
                  val argss   = Seq(Seq(f, emp, sng, uni))
                  val rhs     = core.DefCall(Some(functions), flatFold, targs, argss)
                  val lhs     = api.ValSym(owner, api.TermName.fresh("sng"), rhs.tpe)
                  val lambdas = Seq(Seq(emp), Seq(core.Ref(lhs), uni))
                  val cata    = core.DefCall(xs, API.fold, b, lambdas)
                  child -> (subIndex ++ Seq(lhs -> rhs, child -> cata))
              }

              // Many children - folds to be banana-fused.
              //
              // xs.fold(emp$1)(sng$1, uni$1)
              // ...
              // xs.fold(emp$n)(sng$n, uni$n)
              // =>
              // val emp = (emp$1, ..., emp$n)
              // val sng = x => (sng$1(x), ..., sng$n(x))
              // val uni = (x, y) => (uni$1(x._1, y._1), ..., uni$n(x._n, y._n))
              // val fused = xs.fold(emp)(sng, uni)
              // fused._1
              // ...
              // fused._n
              case (root, children) =>
                val n     = children.size
                val Elem  = api.Type.arg(1, root.info)
                val folds = for ((child, subIndex) <- children) yield subIndex(child)
                val Tuple = api.Type.tupleOf(for ((child, _) <- children) yield child.info)
                val tuple = core.Ref(api.Sym.tuple(n).companion.asModule)
                val apply = tuple.tpe.member(api.TermName.app).asTerm

                val emp = { // (emp$1, ..., emp$n)
                  val lhs   = api.ValSym(owner, api.TermName.fresh("emp"), Tuple)
                  val args  = for (core.DefCall(_, _, _, Seq(Seq(emp), _*)) <- folds) yield emp
                  val rhs   = core.DefCall(Some(tuple), apply, Tuple.typeArgs, Seq(args))
                  lhs -> rhs
                }

                val sng = { // x => (sng$1(x), ..., sng$n(x))
                  val tpe     = api.Type.fun(Seq(Elem), Tuple)
                  val lhs     = api.ValSym(owner, api.TermName.fresh("sng"), tpe)
                  val params  = Seq(api.ParSym(lhs, api.TermName.fresh("x"), Elem))
                  val argss   = Seq(for (p <- params) yield core.ParRef(p))
                  val result  = api.ValSym(lhs, api.TermName.fresh("res"), Tuple)

                  val tmps = for {
                    i <- 1 to n
                    nme = api.TermName.fresh(s"_$i")
                    tpe = api.Type.arg(i, Tuple)
                  } yield api.ValSym(lhs, nme, tpe)

                  val tmpVals = for {
                    (tmp, core.DefCall(_, _, _, Seq(_, Seq(sng, _*)))) <- tmps zip folds
                    apply = sng.tpe.member(api.TermName.app).asTerm
                    rhs   = core.DefCall(Some(sng), apply, argss = argss)
                  } yield core.ValDef(tmp, rhs)

                  val rhs = core.Lambda(params, core.Let(
                    vals = tmpVals :+ core.ValDef(result,
                      core.DefCall(Some(tuple), apply, Tuple.typeArgs,
                        Seq(for (tmp <- tmps) yield core.ValRef(tmp)))),
                    expr = core.Ref(result)))

                  lhs -> rhs
                }


                val uni = { // (x, y) => (uni$1(x._1, y._1), ..., uni$n(x._n, y._n))
                  val tpe    = api.Type.fun(Seq(Tuple, Tuple), Tuple)
                  val lhs    = api.ValSym(owner, api.TermName.fresh("uni"), tpe)
                  val result = api.ValSym(lhs, api.TermName.fresh("res"), Tuple)

                  val params = for (nme <- Seq("x", "y"))
                    yield api.ParSym(lhs, api.TermName.fresh(nme), Tuple)

                  val tmps = for {
                    i <- 1 to n
                    nme = api.TermName.fresh(s"_$i")
                    tpe = api.Type.arg(i, Tuple)
                  } yield api.ValSym(lhs, nme, tpe)

                  val tmpVals = for {
                    ((tmp, i), core.DefCall(_, _, _, Seq(_, Seq(_, uni))))
                      <- tmps.zipWithIndex zip folds

                    args = for {
                      param <- params
                      nme = api.TermName.fresh(param)
                    } yield api.ValSym(param.owner, nme, tmp.info)

                    argVals = for {
                      (arg, param) <- args zip params
                      _i  = param.info.member(api.TermName(s"_${i + 1}")).asTerm
                      rhs = core.DefCall(Some(core.ParRef(param)), _i)
                    } yield core.ValDef(arg, rhs)

                    apply = uni.tpe.member(api.TermName.app).asTerm
                    argss = Seq(for (arg <- args) yield core.ValRef(arg))
                    rhs   = core.DefCall(Some(uni), apply, argss = argss)
                    value <- argVals :+ core.ValDef(tmp, rhs)
                  } yield value

                  val rhs = core.Lambda(params, core.Let(
                    vals = tmpVals :+ core.ValDef(result,
                      core.DefCall(Some(tuple), apply, Tuple.typeArgs,
                        Seq(for (tmp <- tmps) yield core.ValRef(tmp)))),
                    expr = core.Ref(result)))

                  lhs -> rhs
                }

                val fused = api.ValSym(owner, api.TermName.fresh("fused"), Tuple)
                val argss = Seq(Seq(core.Ref(emp._1)), Seq(core.Ref(sng._1), core.Ref(uni._1)))
                val rhs   = core.DefCall(Some(core.Ref(root)), API.fold, Seq(Tuple), argss)

                val projections = for { // fused._i
                  ((child, _), i) <- children.zipWithIndex
                  _i = fused.info.member(api.TermName(s"_${i + 1}")).asTerm
                } yield child -> core.DefCall(Some(core.Ref(fused)), _i)

                fused -> (children.map(_._2).fold(Map.empty)(_ ++ _) ++
                  projections ++ Seq(emp, sng, uni, fused -> rhs))
            }.rootLabel._2
          }.fold(Map.empty)(_ ++ _)

          // Build a new data-flow graph to determine the topological order of vals.
          if (fusedIndex.isEmpty) let else {
            val index = valIndex ++ fusedIndex
            val dataFlow = index.mapValues(_.collect {
              case core.Ref(x) if index.contains(x) => x
            }.toSet)
            val sortedVals = for (lhs <- topoSort(dataFlow).get)
              yield core.ValDef(lhs, index(lhs))
            core.Let(sortedVals, defs, expr)
          }
      }._tree
  }
}
