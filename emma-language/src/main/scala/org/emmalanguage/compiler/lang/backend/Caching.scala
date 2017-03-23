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
package compiler.lang.backend

import compiler.Common
import compiler.lang.core.Core
import compiler.ir.DSCFAnnotations._
import util.Monoids._

import cats.std.all._
import shapeless._

import scala.collection.breakOut

/** Translating to dataflows. */
private[backend] trait Caching extends Common {
  self: Backend with Core =>

  private[backend] object Caching {

    import API._
    import Core.{Lang => core}
    import UniverseImplicits._

    private val runtime = Some(Ops.ref)
    private val cache   = Ops.cache

    /** Is `sym` a method defining a loop or the body of a loop? */
    private def isLoop(sym: u.Symbol) = is.method(sym) &&
      (api.Sym.findAnn[loop](sym).isDefined || api.Sym.findAnn[loopBody](sym).isDefined)

    /** Does `sym` have a type of `DataBag` or a subtype? */
    private def isDataBag(sym: u.Symbol) =
      api.Type.constructor(sym.info) =:= DataBag.tpe

    /** Caches `x` as the new value `y`. */
    private def cacheAs(x: u.TermSymbol, y: u.TermSymbol) = {
      val targs = Seq(api.Type.arg(1, x.info))
      val argss = Seq(Seq(core.Ref(x)))
      core.ValDef(y, core.DefCall(runtime, cache, targs, argss))
    }

    /**
     * Wraps `DataBag` term in `cache(...)` calls if needed.
     *
     * A `DataBag` term needs to be wrapped in a a `cache(...)` call iff
     *
     * 1. it is used more than once;
     * 2. it is passed as argument to a loop method;
     * 3. it is referenced in the closure of a loop method.
     *
     * == Preconditions ==
     *
     * - The input tree is in Emma Core representation.
     *
     * == Postconditions ==
     *
     * - A tree where DataBag have been wrapped in `cache(...)` calls if needed.
     */
    lazy val addCacheCalls: u.Tree => u.Tree = tree => {
      // Per DataBag reference, collect flag for access in a loop and ref count.
      val refs = api.TopDown.withOwnerChain
        .accumulateWith[Map[u.Symbol, (Boolean, Int)]] {
          // Increment counter and set flag if referenced in a loop.
          case Attr.inh(core.BindingRef(target), owners :: _) if isDataBag(target) =>
            val inLoop = owners.reverseIterator.takeWhile(_ != target.owner).exists(isLoop)
            Map(target -> (inLoop, 1))
          // Arguments to loop methods are considered as referenced in the loop.
          case Attr.none(core.DefCall(None, method, _, argss)) if isLoop(method) =>
            argss.flatten.collect { case core.Ref(target) if isDataBag(target) =>
              target -> (true, 1)
            } (breakOut)
        } (merge(tuple2Monoid(disj, implicitly))).traverseAny._acc(tree).head

      /** Cache when referenced in a loop or more than once. */
      def shouldCache(sym: u.Symbol) = {
        val (inLoop, refCount) = refs(sym)
        inLoop || refCount > 1
      }

      /** Creates cached values for parameters of methods and lambdas. */
      def cacheParams(params: Seq[u.ValDef]): Map[u.Symbol, u.ValDef] =
        params.collect { case core.ParDef(p, _) if shouldCache(p) =>
          p -> cacheAs(p, api.ValSym(p.owner, api.TermName.fresh(p), p.info))
        } (breakOut)

      api.TopDown.withParent.accumulate {
        // Accumulate cached lambda and method parameters.
        case core.Lambda(_, params, _) =>
          cacheParams(params)
        case core.DefDef(method, _, paramss, _) if !isLoop(method) =>
          cacheParams(paramss.flatten)
      } (overwrite).transformWith {
        case Attr(let @ core.Let(vals, defs, expr), cached :: _, parent :: _, _) =>
          val params = parent match {
            case Some(core.Lambda(_, params, _)) => params
            case Some(core.DefDef(_, _, paramss, _)) => paramss.flatten
            case _ => Seq.empty
          }

          // Prepend cached parameters, replace cached vals.
          val cachedParams = params.map(_.symbol).collect(cached)
          val cachedVals = vals.flatMap {
            // Don't cache reads.
            case value @ core.ValDef(_, core.DefCall(_, method, _, _))
              if API.DataBag$.ops(method) => Seq(value)
            case core.ValDef(x, rhs) if shouldCache(x) =>
              val y = api.TermSym.fresh(x)
              Seq(core.ValDef(y, rhs), cacheAs(y, x))
            case value => Seq(value)
          }

          if (cachedParams.isEmpty && cachedVals.size == vals.size) let
          else core.Let(cachedParams ++ cachedVals, defs, expr)

        // Dont't replace references in cache(...).
        case Attr.inh(tree, Some(call) :: _) if call.symbol == cache =>
          tree

        // Replace references to cached parameters.
        case Attr.acc(core.Ref(x), cached :: _) if cached.contains(x) =>
          core.Ref(cached(x).symbol.asTerm)
      }._tree(tree)
    }
  }
}
