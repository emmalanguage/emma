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
package compiler.lang.core

import compiler.Common
import compiler.lang.AlphaEq

import scala.Function.const
import scala.util.Random
import scala.util.hashing.MurmurHash3

/** Common subexpression elimination (CSE) for the Core language. */
private[core] trait CSE extends Common {
  self: AlphaEq with Core =>

  import Core.{Lang => core}
  import UniverseImplicits._

  /** Common subexpression elimination (CSE) for the Core language. */
  private[core] object CSE {

    /**
     * Eliminates common subexpressions from a tree.
     *
     * == Preconditions ==
     * - The input `tree` is in ANF (see [[ANF.transform]]).
     *
     * == Postconditions ==
     * - All common subexpressions and corresponding intermediate values are pruned.
     *
     * @param revTab A reverse symbol hash table.
     * @param aliases A running dictionary of value aliases.
     * @return The CSE transformation.
     */
    // FIXME: Is it possible to avoid explicit recursion?
    def transform(
      revTab: Map[HKey, u.TermSymbol],
      aliases: Map[u.Symbol, u.Tree]
    ): TreeTransform = TreeTransform("CSE.transform", api.TopDown.break.transform {
      // Substitute aliases.
      case core.Ref(target) if aliases contains target =>
        aliases(target)

      // Retain unique values, filter common subexpressions.
      case core.Let(vals, defs, expr) =>
        vals.foldLeft(revTab, aliases, Vector.empty[u.ValDef]) {
          case ((tab, dict, uniq), value) => value match {
            // Propagate copies.
            case core.ValDef(x, rhs @ core.Ref(y)) =>
              val alias = dict.getOrElse(y, rhs)
              (tab, dict + (x -> alias), uniq)

            // Propagate constants.
            case core.ValDef(x, core.Atomic(rhs)) =>
              (tab, dict + (x -> rhs), uniq)

            // Eliminate subexpressions.
            case core.ValDef(x, rhs) =>
              // Transform the RHS before hashing.
              val xRhs = transform(tab, dict)(rhs)
              val xVal = valDef(x, xRhs)(dict)
              val hKey = new HKey(hash(xRhs, dict), xVal)
              tab.get(hKey) match {
                // Subexpression already seen, add new alias.
                case Some(y) => (tab, dict + (x -> core.ValRef(y)), uniq)
                // Subexpression unique, add new reverse table entry.
                case _ => (tab + (hKey -> xVal.symbol.asTerm), dict, uniq :+ xVal)
              }
          }
        } match { case (tab, dict, uniq) =>
          // Transform the rest of the let block.
          val xform = transform(tab, dict)
          val xDefs = defs.map(xform).asInstanceOf[Seq[u.DefDef]]
          core.Let(uniq, xDefs, xform(expr))
        }
    } andThen (_.tree) andThen {
      api.Tree.rename(for ((k, core.Ref(v)) <- aliases.toSeq) yield k -> v)
    })

    // ---------------
    // Helper methods
    // ---------------

    private def valDef(sym: u.TermSymbol, rhs: u.Tree)(aliases: Map[u.Symbol, u.Tree]): u.ValDef = {
      val (from, to) = aliases.toList.map(kv => kv._1 -> kv._2.tpe).unzip
      val tpe = sym.info.substituteTypes(from, to)
      if (tpe == sym.info) core.ValDef(sym, rhs)
      else core.ValDef(api.Sym.With(sym)(tpe = tpe), rhs)
    }

    /** Hashes a `tree`, preserving alpha-equivalent `aliases`. */
    private def hash(tree: u.Tree, aliases: Map[u.Symbol, u.Tree]): Int =
      Core.fold(HashAlgebra)(tree)(aliases.mapValues(hash(_, Map.empty)))

    /** A custom map key with a precomputed hash and alpha-equivalent `equals`. */
    private class HKey(private val hash: Int, private val value: u.ValDef) {

      private def lhs =
        value.symbol.asTerm

      override def hashCode(): Int =
        hash

      // If the hashes are equal, check for alpha equivalence.
      override def equals(obj: Any): Boolean = obj match {
        case that: HKey if this.hash == that.hash =>
          val core.ValDef(_, rhs) = that.value
          alphaEq(value, core.ValDef(lhs, rhs)).isGood
        case _ => false
      }
    }

    /** The hash algebra carrier, parametrized by a hash table of symbols. */
    private case class Hash(sym: u.Symbol = u.NoSymbol)(h: Map[u.Symbol, Int] => Int)
      extends (Map[u.Symbol, Int] => Int) {
      def apply(tab: Map[u.Symbol, Int]) = h(tab)
    }

    /** The hash algebra, respecting alpha equivalence. */
    private object HashAlgebra extends Core.Algebra[Hash] {

      import MurmurHash3.mix
      import MurmurHash3.orderedHash
      import MurmurHash3.seqSeed

      // Hash seeds
      private object seed {
        //@formatter:off
        private val rand = new Random(seqSeed)
        // Empty tree
        val empty = rand.nextInt()
        // Atomics
        val lit   = rand.nextInt()
        val this_ = rand.nextInt()
        val ref   = rand.nextInt()
        // Definitions
        val bindingDef = rand.nextInt()
        val defDef     = rand.nextInt()
        // Other
        val typeAscr  = rand.nextInt()
        val termAcc   = rand.nextInt()
        val defCall   = rand.nextInt()
        val inst      = rand.nextInt()
        val lambda    = rand.nextInt()
        val branch    = rand.nextInt()
        val let       = rand.nextInt()
        // Comprehensions
        val comprehend = API.ComprehensionSyntax.comprehension.##
        val generator  = API.ComprehensionSyntax.generator.##
        val guard      = API.ComprehensionSyntax.guard.##
        val head       = API.ComprehensionSyntax.head.##
        //@formatter:on
      }

      // Empty tree
      val empty = Hash() {
        const(seed.empty)
      }

      // Atomics

      def lit(value: Any) = Hash() {
        const(mix(seed.lit, value.##))
      }

      def this_(sym: u.Symbol) = Hash(sym) {
        const(mix(seed.this_, sym.##))
      }

      def ref(sym: u.TermSymbol): Hash = Hash(sym) {
        _.getOrElse(sym, mix(seed.ref, sym.hashCode))
      }

      // Definitions

      def bindingDef(lhs: u.TermSymbol, rhs: Hash) = Hash(lhs) {
        tab => combine(seed.bindingDef)(tab(lhs), hashType(lhs.info), rhs(tab))
      }

      def defDef(sym: u.MethodSymbol,
        tparams: Seq[u.TypeSymbol], paramss: Seq[Seq[Hash]], body: Hash
      ) = Hash(sym) { tab =>
        val hSym = tab(sym)
        // TODO: How to handle type parameters?
        val hTparams = tparams.size.##
        val alphaTab = tab ++ genTab(paramss.flatten, hSym)
        val hParamss = hashSS(alphaTab, paramss)
        combine(seed.defDef)(hSym, hTparams, hParamss, body(alphaTab))
      }

      // Other

      def typeAscr(target: Hash, tpe: u.Type) = Hash(target.sym) {
        const(mix(seed.typeAscr, hashType(tpe)))
      }

      def termAcc(target: Hash, member: u.TermSymbol) = Hash(member) {
        tab => combine(seed.termAcc)(target(tab), member.##)
      }

      def defCall(target: Option[Hash], method: u.MethodSymbol,
        targs: Seq[u.Type], argss: Seq[Seq[Hash]]
      ) = Hash(method) { tab =>
        val hTarget = hashS(tab, target.toSeq)
        val hMethod = tab.getOrElse(method, method.##)
        val hTargs = hashTypes(targs)
        val hArgss = hashSS(tab, argss)
        combine(seed.defCall)(hTarget, hMethod, hTargs, hArgss)
      }

      def inst(target: u.Type, targs: Seq[u.Type], argss: Seq[Seq[Hash]]) =
        Hash(target.typeSymbol) { tab =>
          val hTarget = hashType(target)
          val hTargs = hashTypes(targs)
          val hArgss = hashSS(tab, argss)
          combine(seed.inst)(hTarget, hTargs, hArgss)
        }

      def lambda(sym: u.TermSymbol, params: Seq[Hash], body: Hash) = Hash(sym) { tab =>
        val alphaTab = tab ++ genTab(params)
        val hParams = hashS(alphaTab, params)
        combine(seed.lambda)(hParams, body(alphaTab))
      }

      def branch(cond: Hash, thn: Hash, els: Hash) = Hash() {
        tab => combine(seed.branch)(cond(tab), thn(tab), els(tab))
      }

      def let(vals: Seq[Hash], defs: Seq[Hash], expr: Hash) = Hash() { tab =>
        val alphaTab = tab ++ genTab(vals ++ defs)
        val hVals = hashS(alphaTab, vals)
        val hDefs = hashS(alphaTab, defs)
        combine(seed.let)(hVals, hDefs, expr(alphaTab))
      }

      // Comprehensions

      def comprehend(qs: Seq[Hash], hd: Hash) = Hash() { tab =>
        val alphaTab = tab ++ genTab(qs)
        val hQs = hashS(alphaTab, qs)
        combine(seed.comprehend)(hQs, hd(alphaTab))
      }

      def generator(lhs: u.TermSymbol, rhs: Hash) = Hash(lhs) {
        tab => combine(seed.generator)(tab(lhs), rhs(tab))
      }

      def guard(expr: Hash) = Hash() {
        tab => mix(seed.guard, expr(tab))
      }

      def head(expr: Hash) = Hash() {
        tab => mix(seed.head, expr(tab))
      }

      /** Deterministically combines a sequence of hash values. */
      private def combine(seed: Int)(data: Int*) =
        orderedHash(data, seed)

      /** Applies a sequence of hashes to a given symbol table. */
      private def hashS(tab: Map[u.Symbol, Int], hs: Seq[Hash]) =
        orderedHash(for (h <- hs) yield h(tab))

      /** Applies a nested sequence of hashes to a given symbol table. */
      private def hashSS(tab: Map[u.Symbol, Int], hss: Seq[Seq[Hash]]) =
        orderedHash(for (hs <- hss) yield hashS(tab, hs))

      /** Hashes a type. */
      private def hashType(tpe: u.Type): Int =
        orderedHash(tpe.typeArgs.map(hashType), tpe.typeSymbol.##)

      /** Hashes a sequence of types. */
      private def hashTypes(types: Seq[u.Type]) =
        orderedHash(types.map(hashType))

      /** Generates an update for the hash symbol table from a seed. */
      private def genTab(hs: Seq[Hash], seed: Int = seqSeed) = {
        val rand = new Random(seed)
        hs.map(_.sym) zip Stream.continually(rand.nextInt())
      }
    }
  }
}
