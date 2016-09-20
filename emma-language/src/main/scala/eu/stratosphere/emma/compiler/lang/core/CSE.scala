package eu.stratosphere.emma
package compiler.lang.core

import compiler.Common
import compiler.lang.AlphaEq

import scala.Function.const
import scala.util.Random
import scala.util.hashing.MurmurHash3

/** Common subexpression elimination (CSE) for the Core language. */
private[core] trait CSE extends Common {
  self: AlphaEq with Core =>

  import UniverseImplicits._
  import Core.{Lang => core}

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
    def transform(revTab: Map[HKey, u.TermSymbol], aliases: Map[u.Symbol, u.Tree])
      : u.Tree => u.Tree = api.TopDown.break.transform {
        // Substitute aliases.
        case core.Ref(target) if aliases contains target =>
          aliases(target)

        // Retain unique values, filter common subexpressions.
        case core.Let(vals, defs, expr) =>
          vals.foldLeft(revTab, aliases, Vector.empty[u.ValDef]) {
            case ((tab, dict, uniq), value) => value match {
              // Propagate copies.
              case core.ValDef(x, rhs @ core.Ref(y), _) =>
                val alias = dict.getOrElse(y, rhs)
                (tab, dict + (x -> alias), uniq)

              // Propagate constants.
              case core.ValDef(x, core.Atomic(rhs), _) =>
                (tab, dict + (x -> rhs), uniq)

              // Eliminate subexpressions.
              case core.ValDef(x, rhs, flags) =>
                // Transform the RHS before hashing.
                val xRhs = transform(tab, dict)(rhs)
                val xVal = core.ValDef(x, xRhs, flags)
                val hKey = new HKey(hash(xRhs, dict), xVal)
                tab.get(hKey) match {
                  // Subexpression already seen, add new alias.
                  case Some(y) => (tab, dict + (x -> core.ValRef(y)), uniq)
                  // Subexpression unique, add new reverse table entry.
                  case _ => (tab + (hKey -> x), dict, uniq :+ xVal)
                }
            }
          } match { case (tab, dict, uniq) =>
            // Transform the rest of the let block.
            val xform = transform(tab, dict)
            val xDefs = defs.map(xform).asInstanceOf[Seq[u.DefDef]]
            core.Let(uniq: _*)(xDefs: _*)(xform(expr))
          }
      }.andThen(_.tree)

    // ---------------
    // Helper methods
    // ---------------

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
          val core.ValDef(_, rhs, flags) = that.value
          alphaEq(value, core.ValDef(lhs, rhs, flags)).isGood
        case _ => false
      }
    }

    /** The hash algebra carrier, parametrized by a hash table of symbols. */
    private case class Hash(sym: u.Symbol = u.NoSymbol)(h: Map[u.Symbol, Int] => Int)
      extends (Map[u.Symbol, Int] => Int) {

      override def apply(tab: Map[u.Symbol, Int]): Int = h(tab)
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
        val lit        = rand.nextInt()
        val this_      = rand.nextInt()
        val bindingRef = rand.nextInt()
        val moduleRef  = rand.nextInt()
        // Definitions
        val valDef = rand.nextInt()
        val parDef = rand.nextInt()
        val defDef = rand.nextInt()
        // Other
        val typeAscr = rand.nextInt()
        val defCall  = rand.nextInt()
        val inst     = rand.nextInt()
        val lambda   = rand.nextInt()
        val branch   = rand.nextInt()
        val let      = rand.nextInt()
        // Comprehensions
        val comprehend = ComprehensionSyntax.comprehension.hashCode
        val generator  = ComprehensionSyntax.generator.hashCode
        val guard      = ComprehensionSyntax.guard.hashCode
        val head       = ComprehensionSyntax.head.hashCode
        val flatten    = ComprehensionSyntax.flatten.hashCode
        //@formatter:on
      }

      // Empty tree
      override def empty: Hash = Hash() {
        const(seed.empty)
      }

      // Atomics

      override def lit(value: Any): Hash = Hash() {
        const(mix(seed.lit, value.hashCode))
      }

      override def this_(sym: u.Symbol): Hash = Hash(sym) {
        const(mix(seed.this_, sym.hashCode))
      }

      override def bindingRef(sym: u.TermSymbol): Hash = Hash(sym) {
        _.getOrElse(sym, mix(seed.bindingRef, sym.hashCode))
      }

      override def moduleRef(target: u.ModuleSymbol): Hash = Hash(target) {
        const(mix(seed.moduleRef, target.hashCode))
      }

      // Definitions

      override def valDef(lhs: u.TermSymbol, rhs: Hash, flags: u.FlagSet): Hash = Hash(lhs) {
        tab => combine(seed.valDef)(tab(lhs), hashType(lhs.info), rhs(tab), hashFlags(flags))
      }

      override def parDef(lhs: u.TermSymbol, rhs: Hash, flags: u.FlagSet): Hash = Hash(lhs) {
        tab => combine(seed.parDef)(tab(lhs), hashType(lhs.info), rhs(tab), hashFlags(flags))
      }

      override def defDef(sym: u.MethodSymbol, flags: u.FlagSet,
        tparams: S[u.TypeSymbol], paramss: SS[Hash], body: Hash)
        : Hash = Hash(sym) { tab =>
          val hSym = tab(sym)
          // TODO: How to handle type parameters?
          val hTparams = tparams.size.hashCode
          val alphaTab = tab ++ genTab(paramss.flatten, hSym)
          val hParamss = hashSS(alphaTab, paramss)
          combine(seed.defDef)(hSym, hTparams, hParamss, body(alphaTab), hashFlags(flags))
        }

      // Other

      override def typeAscr(target: Hash, tpe: u.Type): Hash = Hash(target.sym) {
        const(mix(seed.typeAscr, hashType(tpe)))
      }

      override def defCall(target: Option[Hash], method: u.MethodSymbol,
        targs: S[u.Type], argss: SS[Hash])
        : Hash = Hash(method) { tab =>
          val hTarget = hashS(tab, target.toSeq)
          val hMethod = tab.getOrElse(method, method.hashCode)
          val hTargs = hashTypes(targs)
          val hArgss = hashSS(tab, argss)
          combine(seed.defCall)(hTarget, hMethod, hTargs, hArgss)
        }

      override def inst(target: u.Type, targs: Seq[u.Type], argss: SS[Hash])
        : Hash = Hash(target.typeSymbol) { memo =>
          val hTarget = hashType(target)
          val hTargs = hashTypes(targs)
          val hArgss = hashSS(memo, argss)
          combine(seed.inst)(hTarget, hTargs, hArgss)
        }

      override def lambda(sym: u.TermSymbol, params: S[Hash], body: Hash)
        : Hash = Hash(sym) { tab =>
          val alphaTab = tab ++ genTab(params)
          val hParams = hashS(alphaTab, params)
          combine(seed.lambda)(hParams, body(alphaTab))
        }

      override def branch(cond: Hash, thn: Hash, els: Hash): Hash = Hash() {
        tab => combine(seed.branch)(cond(tab), thn(tab), els(tab))
      }

      override def let(vals: S[Hash], defs: S[Hash], expr: Hash)
        : Hash = Hash() { tab =>
          val alphaTab = tab ++ genTab(vals ++ defs)
          val hVals = hashS(alphaTab, vals)
          val hDefs = hashS(alphaTab, defs)
          combine(seed.let)(hVals, hDefs, expr(alphaTab))
        }

      // Comprehensions

      override def comprehend(qs: S[Hash], hd: Hash): Hash = Hash() { tab =>
        val alphaTab = tab ++ genTab(qs)
        val hQs = hashS(alphaTab, qs)
        combine(seed.comprehend)(hQs, hd(alphaTab))
      }

      override def generator(lhs: u.TermSymbol, rhs: Hash): Hash = Hash(lhs) {
        tab => combine(seed.generator)(tab(lhs), rhs(tab))
      }

      override def guard(expr: Hash): Hash = Hash() {
        tab => mix(seed.guard, expr(tab))
      }

      override def head(expr: Hash): Hash = Hash() {
        tab => mix(seed.head, expr(tab))
      }

      override def flatten(expr: Hash): Hash = Hash() {
        tab => mix(seed.flatten, expr(tab))
      }

      /** Deterministically combines a sequence of hash values. */
      private def combine(seed: Int)(data: Int*): Int =
        orderedHash(data, seed)

      /** Applies a sequence of hashes to a given symbol table. */
      private def hashS(tab: Map[u.Symbol, Int], hs: S[Hash]): Int =
        orderedHash(for (h <- hs) yield h(tab))

      /** Applies a nested sequence of hashes to a given symbol table. */
      private def hashSS(tab: Map[u.Symbol, Int], hss: SS[Hash]): Int =
        orderedHash(for (hs <- hss) yield hashS(tab, hs))

      /** Hashes a type. */
      private def hashType(tpe: u.Type): Int =
        orderedHash(tpe.typeArgs.map(hashType), tpe.typeSymbol.hashCode)

      /** Hashes a sequence of types. */
      private def hashTypes(types: Seq[u.Type]): Int =
        orderedHash(types.map(hashType))

      /** Generates an update for the hash symbol table from a seed. */
      private def genTab(hs: S[Hash], seed: Int = seqSeed): Seq[(u.Symbol, Int)] = {
        val rand = new Random(seed)
        hs.map(_.sym) zip Stream.continually(rand.nextInt())
      }

      /** Calculates the hash of a FlagSet, with disregarding the "synthetic" flag and compiler internal flags. */
      private def hashFlags(flags: u.FlagSet): Int = {
        val mods = u.Modifiers(flags)
        MurmurHash3.setHash(FlagsNoSynthetic filter mods.hasFlag)
      }
    }
  }
}
