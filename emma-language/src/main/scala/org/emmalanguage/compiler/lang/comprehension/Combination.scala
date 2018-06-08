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
package compiler.lang.comprehension

import compiler.Common
import compiler.lang.core.Core

import shapeless._

import scala.annotation.tailrec
import scala.collection.breakOut

private[comprehension] trait Combination extends Common {
  self: Core =>

  private[comprehension] object Combination {

    import API._
    import Comprehension._
    import Core.{Lang => core}
    import UniverseImplicits._

    private type Rule = (u.Symbol, u.Tree) => Option[u.Tree]
    private val cs = Comprehension.Syntax(DataBag.sym)
    private val tuple2 = core.Ref(api.Sym.tuple(2).companion.asModule)
    private val tuple2App = tuple2.tpe.member(api.TermName.app).asMethod
    private val Seq(_1, _2) = {
      val tuple2 = api.Type[(Nothing, Nothing)]
      for (i <- 1 to 2) yield tuple2.member(api.TermName(s"_$i")).asTerm
    }

    /**
     * Introduces combinators instead of comprehensions.
     * In addition to the monad ops, it recognizes crosses and equi joins.
     *
     * == Preconditions ==
     *
     * - Input must be in ANF.
     *
     * == Postconditions ==
     *
     * - An ANF tree with no mock-comprehensions.
     */
    val transform = TreeTransform("Combination.transform",
      api.TopDown.withOwner.transformWith {
        case Attr.inh(comp @ cs.Comprehension(_, _), owner :: _) =>
          combine(owner, comp)
      }.andThen(_.tree))

    /**
     * Performs the combination for one comprehension. That is, the root of the given tree
     * must be a comprehension, and it eliminates only this one.
     *
     * Note: We can't just call transform with the Match... rules, because then the application
     * of the rules would be interleaved across the outer and the nested comprehensions, which
     * would mess up the order of rule applications for a single comprehension.
     */
    @tailrec def combine(owner: u.Symbol, tree: u.Tree): u.Tree =
      rules.foldLeft(Option.empty[u.Tree]) {
        (done, rule) => done orElse rule(owner, tree)
      } match {
        case Some(result) => combine(owner, result)
        case None => tree
      }

    /**
     * Creates a filter combinator.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | qs1, x ← xs, qs2, p x, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     $qs2
     *     guard {
     *       $pVals
     *       $pExpr
     *     }
     *     $qs3
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The matched guard must use at most one generator variable and this variable should be `x`.
     *
     * ==Rewrite==
     * {{{ [[ hd | qs1, x ← filter p xs, qs2, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       val $p = x => {
     *         $pVals
     *         $pExpr
     *       }
     *       val $filtered = $xExpr withFilter $p
     *       $filtered
     *     }
     *     $qs2
     *     $qs3
     *     $hd
     *   }
     * }}}
     */
    val MatchFilter: Rule = {
      case (_, cs.Comprehension(qs, hd)) => (for {
        guard @ cs.Guard(pred) <- qs.view
        (qs12, qs3) = splitAt(guard)(qs)
        xGen @ cs.Generator(x, xRhs) <- qs12.view
        (qs1, qs2) = splitAt[u.Tree](xGen)(qs12)
        refd = api.Tree.refs(guard) intersect gens(qs12)
        if refd.isEmpty || (refd.size == 1 && refd.head == x)
      } yield {
        val gen = cs.Generator(x, Core.mapSuffix(xRhs) { (xVals, xExpr) =>
          val (pRef, pVal) = valRefAndDef(x, "p", core.Lambda(Seq(x), pred))
          val (fRef, fVal) = valRefAndDef(x, "filtered", cs.WithFilter(xExpr)(pRef))
          val vals = Seq.concat(xVals, Seq(pVal, fVal))
          core.Let(vals, expr = fRef)
        })
        val combined = Seq.concat(qs1, Seq(gen), qs2, qs3)
        cs.Comprehension(combined, hd)
      }).headOption

      case _ => None
    }


    /**
     * Creates a flatMap combinator.
     *
     * ===Matching Pattern===
     * {{{ [[ hd | qs1, x ← xs, qs2, y ← f x, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     $qs2
     *     val $y = generator yRhs
     *     $qs3
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The generator of `y` must refer to exactly one generator variable - `x`.
     * - The remaining qualifiers, as well as the head should not refer to `x`.
     *
     * ==Rewrite==
     * {{{ [[ hd | qs1, qs2, y ← flatMap f xs, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     $qs2
     *     val $y = generator {
     *       $xVals
     *       val $f = $x => yBody
     *       val $fmapped = $xExpr flatMap $f
     *       $fmapped
     *     }
     *     $qs3
     *     $hd
     *   }
     * }}}
     */
    val MatchFlatMap1: Rule = {
      case (_, cs.Comprehension(qs, hd)) => (for {
        xGen @ cs.Generator(x, xRhs) <- qs.view
        (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, yRhs) <- qs23.view
        (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
        if (api.Tree.refs(yGen) intersect gens(qs)) == Set(x)
        if Seq.concat(qs2, qs3, Seq(hd)).forall(!api.Tree.refs(_).contains(x))
      } yield {
        val rhs = Core.mapSuffix(xRhs, Some(yRhs.tpe)) { (xVals, xExpr) =>
          val (fRef, fVal) = valRefAndDef(y, "f", core.Lambda(Seq(x), yRhs))
          val (fmRef, fmVal) = valRefAndDef(y, "fmapped", cs.FlatMap(xExpr)(fRef))
          val vals = Seq.concat(xVals, Seq(fVal, fmVal))
          core.Let(vals, expr = fmRef)
        }
        val gen = cs.Generator(y, rhs)
        val combined = Seq.concat(qs1, qs2, Seq(gen), qs3)
        cs.Comprehension(combined, hd)
      }).headOption

      case _ => None
    }

    /**
     * Creates a flatMap combinator. The difference between this and the other flatMap rule, is that
     * this is able to handle the situation when x is referenced in later qualifiers or the head.
     *
     * ===Matching Pattern===
     * {{{ [[ hd | qs1, x ← xs, qs2, y ← f x, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     $qs2
     *     val $y = generator {
     *       $yVals
     *       $yExpr
     *     }
     *     $qs3
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The generator of `y` must refer to exactly one generator variable - `x`.
     *
     * ==Rewrite==
     * {{{
     *   comprehension {
     *     $qs1
     *     val $xy = generator {
     *       $xVals
     *       val $f = $x => {
     *         $yVals
     *         val $g = $y1 => {
     *           val $ir = ($x, $y1)
     *           $ir
     *         }
     *         val $mapped = $yExpr map $g
     *         $mapped
     *       }
     *       val $fmapped = $xExpr flatMap $f
     *       $fmapped
     *     }
     *     $qs2[xy._1/x][xy._2/y]  // Note: Introduce ValDefs for xy._1 and xy._2 to maintain ANF
     *     $qs3[xy._1/x][xy._2/y]
     *     $hd[xy._1/x][xy._2/y]
     *   }
     * }}}
     */
    val MatchFlatMap2: Rule = {
      case (owner, cs.Comprehension(qs, hd)) => (for {
        xGen @ cs.Generator(x, xRhs) <- qs.view
        (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, yRhs) <- qs23.view
        (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
        if (api.Tree.refs(yGen) intersect gens(qs)) == Set(x)
      } yield {
        val xyTpe = api.Type.kind2[Tuple2](x.info, y.info)
        val xyBag = Some(api.Type(DataBag.tpe, Seq(xyTpe)))
        val xy = api.ValSym(owner, api.TermName.fresh("xy"), xyTpe)
        val xyRef = core.Ref(xy)
        val xy1 = core.ValDef(x, core.DefCall(Some(xyRef), _1, Seq.empty, Seq.empty))
        val xy2 = core.ValDef(y, core.DefCall(Some(xyRef), _2, Seq.empty, Seq.empty))
        val bind = capture(cs, Seq(xy1, xy2)) _
        val y1 = api.TermSym.fresh(y)
        val irArgss = Seq(Seq(core.Ref(x), core.Ref(y1)))
        val irRhs = core.DefCall(Some(tuple2), tuple2App, Seq(x.info, y.info), irArgss)
        val (irRef, irVal) = valRefAndDef(xy, "ir", irRhs)
        val gBody = core.Let(Seq(irVal), expr = irRef)
        val (gRef, gVal) = valRefAndDef(xy, "g", core.Lambda(Seq(y1), gBody))
        val fBody = Core.mapSuffix(yRhs, xyBag) { (yVals, yExpr) =>
          val (mRef, mVal) = valRefAndDef(xy, "mapped", cs.Map(yExpr)(gRef))
          core.Let(yVals ++ Seq(gVal, mVal), expr = mRef)
        }
        val (fRef, fVal) = valRefAndDef(xy, "f", core.Lambda(Seq(x), fBody))
        val gen = cs.Generator(xy, Core.mapSuffix(xRhs, xyBag) { (xVals, xExpr) =>
          val (fmRef, fmVal) = valRefAndDef(xy, "fmapped", cs.FlatMap(xExpr)(fRef))
          val vals = Seq.concat(xVals, Seq(fVal, fmVal))
          core.Let(vals, expr = fmRef)
        })
        val combined = Seq.concat(qs1, Seq(gen), qs2.view map bind, qs3.view map bind)
        cs.Comprehension(combined, bind(hd))
      }).headOption

      case _ => None
    }

    /**
     * Creates a cross combinator.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | qs1, x ← xs, y ← ys, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     val $y = generator {
     *       $yVals
     *       $yExpr
     *     }
     *     $qs3
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The matched generators should not be correlated, i.e. the gen of `y` should not refer `x`.
     * - The matched generators should not have control flow.
     *
     * ==Rewrite==
     * {{{ [[ hd[xy._1/x][xy._2/y] | qs1, xy ← ⨯ xs ys, qs3[xy._1/x][xy._2/y] ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $xy = generator {
     *       $xVals
     *       $yVals
     *       val $crossed = cross($xExpr, $yExpr)
     *       $crossed
     *     }
     *     $qs3[xy._1/x][xy._2/y]  // Note: Introduce ValDefs for xy._1 and xy._2 to maintain ANF
     *     $hd[xy._1/x][xy._2/y]
     *   }
     * }}}
     */
    val MatchCross: Rule = {
      case (owner, cs.Comprehension(qs, hd)) => (for {
        xGen @ cs.Generator(x, xRhs) <- qs.view
        (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, yRhs) <- qs23.view
        (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
        if qs2.isEmpty && !api.Tree.refs(yGen).contains(x)
      } yield {
        val xyTpe = api.Type.kind2[Tuple2](x.info, y.info)
        val xyBag = Some(api.Type(DataBag.tpe, Seq(xyTpe)))
        val xy = api.ValSym(owner, api.TermName.fresh("xy"), xyTpe)
        val xyRef = core.Ref(xy)
        val xy1 = core.ValDef(x, core.DefCall(Some(xyRef), _1, Seq.empty, Seq.empty))
        val xy2 = core.ValDef(y, core.DefCall(Some(xyRef), _2, Seq.empty, Seq.empty))
        val bind = capture(cs, Seq(xy1, xy2)) _
        val gen = cs.Generator(xy, Core.mapSuffix(xRhs, xyBag) { (xVals, xExpr) =>
          Core.mapSuffix(yRhs, xyBag) { (yVals, yExpr) =>
            val (cRef, cVal) = valRefAndDef(xy, "crossed", Combinators.Cross(xExpr, yExpr))
            val vals = Seq.concat(xVals, yVals, Seq(cVal))
            core.Let(vals, Seq.empty, cRef)
          }
        })
        val combined = Seq.concat(qs1, Seq(gen), qs3.view map bind)
        cs.Comprehension(combined, bind(hd))
      }).headOption

      case _ => None
    }

    // =======================================
    // The following four rules transform guards for better join combination.
    // We partially use rules for normalization into conjunctive normal form.
    //
    // 1. Eliminate double negation (!!x => x);
    // 2. Move negation inside disjunction (!(x || y) => !x && !y)
    // 3. Split guards that return a conjunction in two guards. Repeat from 1. in sub-guards.
    // 4. Collect all mutually independent guards of the form fi(x) == gi(y) with x and y fixed, and replace with
    //    ((f1(x), ... , fn(x)) == (g1(y), ... , gn(y)))
    //
    // We don't use full CNF, because once a non-negated disjunction is involved, it cannot be processed any
    // further. Thus [!(x && y) => !x || !y] and demorgan [ x || (y && z) => (x || y) && (x || z) ] are not useful
    // for joins.
    // =======================================

    /**
     * Removes double negations from guards.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | qs1, ! (! expr), qs2 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     guard {
     *       $grdVals
     *       val $neg1 = unary_! $Expr
     *       val $neg2 = unary_! $neg1
     *       $neg2
     *     }
     *     $qs2
     *     $hd
     *   }
     * }}}
     *
     * ==Rewrite==
     * {{{ [[ hd | qs1, expr, qs2 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     guard {
     *       $grdVals
     *       $Expr
     *     }
     *     $qs2
     *     $hd
     *   }
     * }}}
     */
    val MatchDoubleNegation: Rule = {
      case (_, cs.Comprehension(qs, hd)) => (for {
        guard @ cs.Guard(core.Let(grdVals, Seq(), core.Ref(retSym))) <- qs.view
        (qs1, qs2) = splitAt(guard)(qs)
        core.ValDef(condNeg, core.DefCall(Some(opd), negSym, _, _)) <- grdVals
        if negSym.name.toString == "unary_$bang"
        core.ValDef(`retSym`, core.DefCall(Some(core.Ref(`condNeg`)), `negSym`, _, _)) <- grdVals
      } yield {
        val newGuard = Core.dce(cs.Guard(core.Let(grdVals, Seq(), opd)))
        val newTree = Seq.concat(qs1, Seq(newGuard), qs2)
        val out = cs.Comprehension(newTree, hd)
        out
      }).headOption

      case _ => None
    }

    /**
     * De Morgan for negation of disjunction.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | qs1, ! (expr1 || expr2), qs2 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     guard {
     *       $grdVals
     *       $disj = expr1 || expr2
     *       $neg = ! $disj
     *       $neg
     *     }
     *     $qs2
     *     $hd
     *   }
     * }}}
     *
     * ==Rewrite==
     * {{{ [[ hd | qs1, !expr1 && !expr2, qs2 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     guard {
     *       $grdVals
     *       $neg1 = ! $expr1
     *       $neg2 = ! $expr2
     *       $conj = neg1 && neg2
     *       $conj
     *     }
     *     $qs2
     *     $hd
     *   }
     * }}}
     */
    val MatchDeMorgan: Rule = {
      case (owner, cs.Comprehension(qs, hd)) => (for {
        guard @ cs.Guard(core.Let(grdVals, Seq(), core.Ref(retSym))) <- qs.view
        (qs1, qs2) = splitAt(guard)(qs)
        core.ValDef(disjTS, core.DefCall(Some(disjLhs), disjMS, _, Seq(Seq(disjRhs)))) <- grdVals
        if disjMS.name.toString == "$bar$bar"
        core.ValDef(`retSym`, core.DefCall(Some(core.Ref(`disjTS`)), negMS, _, _)) <- grdVals
        if negMS.name.toString == "unary_$bang"
      } yield {
        val negLhs = core.DefCall(Some(disjLhs), negMS)
        val negRhs = core.DefCall(Some(disjRhs), negMS)
        val (negLhsRef, negLhsDef) = valRefAndDef(owner, negMS.name.toString, negLhs)
        val (negRhsRef, negRhsDef) = valRefAndDef(owner, negMS.name.toString, negRhs)

        val andMS = disjTS.info.member(api.TermName("&&")).alternatives.collectFirst({
          case api.DefSym(m) if m.isPublic => m
        }).get

        val result = core.DefCall(Some(negLhsRef), andMS, argss = Seq(Seq(negRhsRef)))
        val (resultRef, resultDef) = valRefAndDef(owner, andMS.name.toString, result)
        val newGrdVals = grdVals ++ Seq(negLhsDef, negRhsDef, resultDef)
        val newGuard = Core.dce(cs.Guard(core.Let(newGrdVals, Seq(), resultRef)))
        val newTree = Seq.concat(qs1, Seq(newGuard), qs2)
        cs.Comprehension(newTree, hd)
      }).headOption

      case _ => None
    }

    /**
     * Splits conjunctions within guards into two guards.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | qs1, expr1 && expr2, qs2 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     guard {
     *       $grdVals
     *       $conj = $expr1 && $expr2
     *       $conj
     *     }
     *   }
     * }}}
     *
     * ==Rewrite==
     * {{{ [[ hd | qs2, expr1, expr2, qs2 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     guard {
     *       $grdVals1
     *       $expr1
     *     }
     *     guard {
     *       $grdVals2
     *       $expr2
     *     }
     *     $qs2
     *     $hd
     *   }
     * }}}
     */
    val MatchSplitGuard: Rule = {
      case (owner, cs.Comprehension(qs, hd)) => (for {
        guard @ cs.Guard(core.Let(grdVals, Seq(), core.Ref(retSym))) <- qs.view
        (qs1, qs2) = splitAt(guard)(qs)

        core.ValDef(`retSym`, core.DefCall(Some(lhsRef), conjMs, _, Seq(Seq(rhsRef)))) <- grdVals
        if conjMs.name.toString == "$amp$amp"
      } yield {
        val newGuard1 = Core.dce(cs.Guard(core.Let(grdVals, Seq(), lhsRef)))
        val newGuard2 = Core.dce(cs.Guard(core.Let(grdVals, Seq(), rhsRef)))

        val newTree = Seq.concat(qs1, Seq(newGuard1, newGuard2), qs2)
        cs.Comprehension(newTree, hd)
      }).headOption

      case _ => None
    }

    /**
     * Collects equality guards.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | qs1, expr1 == expr2, qs2, expr3 == expr4, qs3, ... , qsN ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     guard {
     *       $grdVals1
     *       $eq1 = expr1 == expr2
     *       $eq1
     *     }
     *     $qs2
     *     guard {
     *       $grdVals2
     *       $eq2 = expr3 == expr4
     *       $eq2
     *     }
     *     $qs3
     *     guard { ...
     *     ...
     *     $qsN
     *     $hd
     *   }
     * }}}
     *
     * ==Rewrite==
     * {{{ [[ hd | qs1, qs2, ..., qsN, (expr1, expr3, ...) == (expr2, expr4, ...) ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     $qs2
     *     $qs3
     *     ...
     *     $qsN
     *     guard {
     *       $grdVals
     *       $tuple1 = ($expr1, $expr3, ...)
     *       $tuple2 = ($expr2, $expr4, ...)
     *       $eq = $tuple1 && $tuple2
     *       $eq
     *     }
     *     $hd
     *   }
     * }}}
     */
    val MatchCollectEqualityGuards: Rule = {
      case (owner, cs.Comprehension(qs, hd)) => ( for {
        xGen @ cs.Generator(x, xRhs) <- qs.view
        (_, qs23) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, yRhs) <- qs23.view
        (_, qs3) = splitAt[u.Tree](yGen)(qs23)

        guards = (for {
          grd @ cs.Guard(core.Let(grdVals, Seq(), core.Ref(retSym))) <- qs3.view
          if (api.Tree.refs(grd) intersect gens(qs)) == Set(x, y)
          condVal @ core.ValDef(`retSym`, core.DefCall(Some(eqLhs), eqMs, _, Seq(Seq(eqRhs)))) <- grdVals
          if eqMs.name.toString == "$eq$eq"

          joinVals = grdVals.filter(_ != condVal)
          (kxExpr, kyExpr) <- Seq((eqLhs, eqRhs), (eqRhs, eqLhs))
          kxBody = Core.dce(core.Let(joinVals, Seq.empty, kxExpr))
          if !api.Tree.refs(kxBody).contains(y)
          kyBody = Core.dce(core.Let(joinVals, Seq.empty, kyExpr))
          if !api.Tree.refs(kyBody).contains(x)

        } yield {
          (grd, eqLhs, eqRhs, grdVals)
        }).toList

        if guards.size > 1
      } yield {
        val guardsFull = guards.map(_._1)
        val rest = qs.filter(!guardsFull.contains(_))

        val lhsVals = guards.map { _._2 }
        val rhsVals = guards.map { _._3 }
        val valDefs = guards.flatMap { _._4 }

        val n = lhsVals.size

        val lhsTypes = lhsVals.map(t => t.tpe)
        val lhsTuple = core.Ref(api.Sym.tuple(n).companion.asModule)
        val lhsApp = lhsTuple.tpe.member(api.TermName.app).asMethod
        val lhsT = core.DefCall(Some(lhsTuple), lhsApp, lhsTypes, Seq(lhsVals))
        val (lhsRef, lhsDef) = valRefAndDef(owner, "tuple" + n.toString, lhsT)

        val rhsTypes = rhsVals.map(t => t.tpe)
        val rhsTuple = core.Ref(api.Sym.tuple(n).companion.asModule)
        val rhsApp = rhsTuple.tpe.member(api.TermName.app).asMethod
        val rhsT = core.DefCall(Some(rhsTuple), rhsApp, rhsTypes, Seq(rhsVals))
        val (rhsRef, rhsDef) = valRefAndDef(owner, "tuple" + n.toString, rhsT)

        val eqMs = api.Type.tupleOf(rhsTypes).member(api.TermName("==")).alternatives.collectFirst({
          case api.DefSym(m) if m.isPublic => m
        }).get

        val result = core.DefCall(Some(lhsRef), eqMs, Seq(), Seq(Seq(rhsRef)))
        val (resRef, resDef) = valRefAndDef(owner, eqMs.name.toString, result)
        val newGuard = Core.dce(cs.Guard(core.Let(valDefs ++ Seq(lhsDef, rhsDef, resDef), Seq(), resRef)))

        val newTree = Seq.concat(rest, Seq(newGuard))
        cs.Comprehension(newTree, hd)
      }).headOption

      case _ => None
    }

    // =================================================
    // end of guard transformations for join combination
    // =================================================

    /**
     * Creates a join combinator.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | qs1, x ← xs, y ← ys, qs3, k₁ x == k₂ y, qs4 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     val $y = generator {
     *       $yVals
     *       $yExpr
     *     }
     *     $qs3
     *     guard {
     *       $joinVals
     *       val $cond = $kxExpr == $kyExpr  // Or `$kyExpr == $kxExpr`
     *       $cond
     *     }
     *     $qs4
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The matched generators should not be correlated, i.e. the gen of `y` should not refer `x`.
     * - The matched guard should refer to x and y, but not to other generator variables.
     * - $kxExpr's dependencies in $joinCondVals0 should not contain y, and vice versa for $kyExpr
     * - The matched generators and guard should not have control flow.
     *
     * ==Rewrite==
     * {{{ [[ hd[xy._1/x][xy._2/y] | qs1, xy ← ⋈ k₁ k₂ xs ys, qs3[xy._1/x][xy._2/y],
     *        qs4[xy._1/x][xy._2/y] ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $xy = generator {
     *       $xVals
     *       $yVals
     *       val $kx = x => {
     *         $joinVals  // But only those vals that are needed for kxExpr
     *         $kxExpr
     *       }
     *       val $ky = y => {
     *         $joinVals  // But only those vals that are needed for kyExpr
     *         $kyExpr
     *       }
     *       val $joined = join $kx $ky $xExpr $yExpr
     *       $joined
     *     }
     *     $qs3[xy._1/x][xy._2/y]  // Note: Introduce ValDefs for j.x and j.y to maintain ANF
     *     $qs4[xy._1/x][xy._2/y]
     *     $hd[xy._1/x][xy._2/y]
     *   }
     * }}}
     */
    val MatchEquiJoin: Rule = {
      case (owner, cs.Comprehension(qs, hd)) => (for {
        xGen @ cs.Generator(x, xRhs) <- qs.view
        (qs1, qs234) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, yRhs) <- qs234.view
        (qs2, qs34) = splitAt[u.Tree](yGen)(qs234)
        if qs2.isEmpty && !api.Tree.refs(yGen).contains(x)
        grd @ cs.Guard(core.Let(grdVals, Seq(), core.Ref(cond))) <- qs34.view
        if (api.Tree.refs(grd) intersect gens(qs)) == Set(x, y)
        condVal @ core.ValDef(`cond`, core.DefCall(Some(eqLhs), eq, _, Seq(Seq(eqRhs)))) <- grdVals
        if eq.name.toString == "$eq$eq"
        joinVals = grdVals.filter(_ != condVal)
        (kxExpr, kyExpr) <- Seq((eqLhs, eqRhs), (eqRhs, eqLhs))
        kxBody = Core.dce(core.Let(joinVals, Seq.empty, kxExpr))
        if !api.Tree.refs(kxBody).contains(y)
        kyBody = Core.dce(core.Let(joinVals, Seq.empty, kyExpr))
        if !api.Tree.refs(kyBody).contains(x)
        (qs3, qs4) = splitAt[u.Tree](grd)(qs34)
      } yield {
        // Functions are covariant in the return type, therefore it is safe for key functions to
        // return a subtype of the key type. However, in case of primitive promotion (e.g. Int to
        // Double) the subtype relation is weak and explicit casting is necessary.
        val kLub = api.Type.weakLub(Seq(kxExpr.tpe, kyExpr.tpe))
        def maybeCast(tree: u.Tree) = if (tree.tpe <:< kLub) tree else tree match {
          case core.Let(valDefs, _, expr) =>
            val asInstanceOf = expr.tpe.member(api.TermName("asInstanceOf")).asTerm
            val cast = core.DefCall(Some(expr), asInstanceOf, Seq(kLub), Seq.empty)
            val (castRef, castVal) = valRefAndDef(owner, "cast", cast)
            core.Let(valDefs :+ castVal, Seq.empty, castRef)
          case other => other
        }

        val xyTpe = api.Type.kind2[Tuple2](x.info, y.info)
        val xyBag = Some(api.Type.apply(DataBag.tpe, Seq(xyTpe)))
        val xy = api.ValSym(owner, api.TermName.fresh("xy"), xyTpe)
        val xyRef = core.Ref(xy)
        val xy1 = core.ValDef(x, core.DefCall(Some(xyRef), _1, Seq.empty, Seq.empty))
        val xy2 = core.ValDef(y, core.DefCall(Some(xyRef), _2, Seq.empty, Seq.empty))
        val bind = capture(cs, Seq(xy1, xy2)) _
        val (kxRef, kxVal) = valRefAndDef(xy, "kx", core.Lambda(Seq(x), maybeCast(kxBody)))
        val (kyRef, kyVal) = valRefAndDef(xy, "ky", core.Lambda(Seq(y), maybeCast(kyBody)))
        val gen = cs.Generator(xy, Core.mapSuffix(xRhs, xyBag) { (xVals, xExpr) =>
          Core.mapSuffix(yRhs, xyBag) { (yVals, yExpr) =>
            val join = Combinators.EquiJoin(kxRef, kyRef)(xExpr, yExpr)
            val (jRef, jVal) = valRefAndDef(xy, "joined", join)
            val vals = Seq.concat(xVals, yVals, Seq(kxVal, kyVal, jVal))
            core.Let(vals, Seq.empty, jRef)
          }
        })
        val combined = Seq.concat(qs1, Seq(gen), qs3.view map bind, qs4.view map bind)
        cs.Comprehension(combined, bind(hd))
      }).headOption

      case _ => None
    }

    /**
     * Eliminates the residual comprehension.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | x <- xs ]] }}}
     * {{{
     *   comprehension {
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     $hd
     *   }
     * }}}
     *
     * ==Rewrite==
     * {{{ map hd xs }}}
     * {{{
     *   $xVals
     *   val $f = $fArg => $hd[fArg/x]
     *   val $mapped = $xExpr map $f
     *   $mapped
     * }}}
     */
    val MatchMap: Rule = {
      case (owner, cs.Comprehension(Seq(cs.Generator(x, rhs)), cs.Head(hd))) =>
        val tpe = if (x.info =:= hd.tpe) None else Some(api.Type(DataBag.tpe, Seq(hd.tpe)))
        Some(Core.mapSuffix(rhs, tpe) { (vals, expr) =>
          val (fRef, fVal) = valRefAndDef(owner, "f", core.Lambda(Seq(x), hd))
          val (mRef, mVal) = valRefAndDef(owner, "mapped", cs.Map(expr)(fRef))
          core.Let(vals ++ Seq(fVal, mVal), expr = mRef)
        })
      case _ => None
    }

    private val rules = Seq(
      MatchFilter, MatchFlatMap1, MatchFlatMap2,
      MatchDoubleNegation, MatchDeMorgan, MatchSplitGuard, MatchCollectEqualityGuards,
      MatchEquiJoin, MatchCross, MatchMap)

    /** Creates a ValDef, and returns its Ident on the left hand side. */
    private def valRefAndDef(own: u.Symbol, name: String, rhs: u.Tree): (u.Ident, u.ValDef) = {
      val lhs = api.ValSym(own, api.TermName.fresh(name), rhs.tpe)
      (core.Ref(lhs), core.ValDef(lhs, rhs))
    }

    private def gens(qs: Seq[u.Tree]): Set[u.TermSymbol] =
      qs.collect { case cs.Generator(x, _) => x } (breakOut)
  }
}
