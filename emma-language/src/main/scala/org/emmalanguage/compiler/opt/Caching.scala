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

import compiler.Common
import compiler.ir.DSCFAnnotations._
import compiler.lang.core.Core
import util.Monoids._

import cats.instances.all._
import shapeless._

import scala.collection.breakOut

/** Translating to dataflows. */
private[opt] trait Caching extends Common {
  self: Core =>

  private[opt] object Caching {

    import API._
    import Core.{Lang => core}
    import UniverseImplicits._

    private type CacheFlags = Map[u.Symbol, (Boolean, Int)]
    private type CacheStore = Map[u.Symbol, u.ValDef]

    private val runtime = Some(Ops.ref)
    private val cache   = Ops.cache

    /** Does `sym` have a type of `DataBag` or a subtype? */
    private def isDataBag(sym: u.Symbol) =
      api.Type.constructor(sym.info) =:= DataBag.tpe

    /** Is `sym` a method defining a loop or the body of a loop? */
    private def isLoop(sym: u.Symbol) = is.method(sym) &&
      (api.Sym.findAnn[loop](sym).isDefined || api.Sym.findAnn[loopBody](sym).isDefined)

    /** Is `sym` a method defining a suffix continuation? */
    private def isSuffix(sym: u.Symbol) = is.method(sym) &&
      api.Sym.findAnn[suffix](sym).isDefined

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
    lazy val addCacheCalls = TreeTransform("Caching.addCacheCalls", tree => {
      // Per DataBag reference, collect flag for access in a loop and ref count.
      val refs = api.TopDown.withOwnerChain.accumulateWith[CacheFlags] {
        // Increment counter and set flag if referenced in a loop.
        case Attr.inh(core.BindingRef(target), owners :: _) if isDataBag(target) =>
          val inLoop = owners.reverseIterator.takeWhile {
            owner => owner != target.owner && !isSuffix(owner)
          }.exists(isLoop)
          Map(target -> (inLoop, 1))
        // Arguments to loop methods are considered as referenced in the loop.
        case Attr.none(core.DefCall(None, method, _, argss)) if isLoop(method) =>
          argss.flatten.collect { case core.Ref(target) if isDataBag(target) =>
            target -> (true, 1)
          } (breakOut)
      } (merge(catsKernelStdMonoidForTuple2(disj, implicitly))).traverseAny._acc(tree).head

      /* Cache when referenced in a loop or more than once. */
      def shouldCache(x: u.Symbol) = {
        val (inLoop, refCount) = refs(x)
        inLoop || refCount > 1
      }

      /* Creates cached values for parameters of methods and lambdas. */
      def cacheBindings(binds: Seq[u.ValDef]): CacheStore =
        binds.collect { case core.BindingDef(x, _) if shouldCache(x) =>
          val y = api.ValSym(x.owner, api.TermName.fresh(x), x.info.widen)
          val targs = Seq(api.Type.arg(1, x.info))
          val argss = Seq(Seq(core.Ref(x)))
          x -> core.ValDef(y, core.DefCall(runtime, cache, targs, argss))
        } (breakOut)

      api.TopDown.withParent.accumulate[CacheStore] {
        case core.Lambda(_, params, _) =>
          cacheBindings(params)
        case core.DefDef(method, _, paramss, _) if !isLoop(method) =>
          cacheBindings(paramss.flatten)
        case core.Let(vals, _, _) =>
          cacheBindings(vals.filter {
            case core.ValDef(_, core.DefCall(_, m, _, _)) => !API.DataBag$.ops(m)
            case _ => true
          })
      } (overwrite).transformWith {
        // Prepend cached parameters, replace cached values.
        case Attr(let @ core.Let(vals, defs, expr), cached :: _, parent :: _, _) =>
          val cachedParams = (parent match {
            case Some(core.Lambda(_, ps, _)) => ps
            case Some(core.DefDef(_, _, pss, _)) => pss.flatten
            case _ => Seq.empty
          }).map(_.symbol).collect(cached)

          val cachedVals = vals.flatMap { v =>
            v +: cached.get(v.symbol).toSeq
          }

          if (cachedParams.isEmpty && cachedVals.size == vals.size) let
          else core.Let(cachedParams ++ cachedVals, defs, expr)

        // Dont't replace references in cache(...).
        case Attr.inh(t, Some(call) :: _)
          if call.symbol == cache => t

        // Replace references to cached parameters or values.
        case Attr.acc(core.Ref(x), cached :: _) if cached.contains(x) =>
          core.Ref(cached(x).symbol.asTerm)
      }._tree(tree)
    })
  }
}
