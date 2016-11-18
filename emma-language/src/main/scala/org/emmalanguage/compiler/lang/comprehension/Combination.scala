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
import util.Functions._

import shapeless._

import scala.annotation.tailrec
import scala.collection.breakOut

private[comprehension] trait Combination extends Common {
  self: Core with Comprehension =>

  private[comprehension] object Combination {

    import UniverseImplicits._
    import Comprehension.{splitAt, Combinators}
    import Core.{Lang => core}

    private type Rule = (u.Symbol, u.Tree) =?> u.Tree
    private val cs = new Comprehension.Syntax(API.bagSymbol)
    private val tuple2 = core.Ref(api.Sym.tuple(2).companion.asModule)
    private val tuple2App = tuple2.tpe.member(api.TermName.app).asMethod
    private val Seq(_1, _2) = {
      val tuple2 = api.Type[(Nothing, Nothing)]
      for (i <- 1 to 2) yield tuple2.member(api.TermName(s"_$i")).asTerm
    }

    // TODO: Split conjunctive filter predicates.

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
    lazy val transform: u.Tree => u.Tree =
      api.TopDown.withOwner.transformWith {
        case Attr.inh(comp @ cs.Comprehension(_, _), owner :: _) =>
          combine(owner, comp)
      }.andThen(_.tree)

    /**
     * Performs the combination for one comprehension. That is, the root of the given tree
     * must be a comprehension, and it eliminates only this one.
     *
     * Note: We can't just call transform with the Match... rules, because then the application
     * of the rules would be interleaved across the outer and the nested comprehensions, which
     * would mess up the order of rule applications for a single comprehension.
     */
    def combine(owner: u.Symbol, tree: u.Tree) = {
      var root = tree

      //@formatter:off
      // states for the state machine
      sealed trait RewriteState
      object Start     extends RewriteState
      object Filter    extends RewriteState
      object FlatMap   extends RewriteState
      object FlatMap2  extends RewriteState
      object Join      extends RewriteState
      object Cross     extends RewriteState
      object Residuals extends RewriteState
      object End       extends RewriteState

      def applyOnce(rule: Rule): Boolean = {
        val prevRoot = root
        root = complete(rule)(owner, root)(root)
        root != prevRoot
      }

      @tailrec
      def applyExhaustively(rule: Rule): Unit = {
        if (applyOnce(rule)) applyExhaustively(rule)
      }

      // state machine for the rewrite process
      @tailrec
      def process(state: RewriteState): u.Tree = state match {
        case Start     => process(Filter)
        case Filter    => applyExhaustively(MatchFilter); process(FlatMap)
        case FlatMap   => if (applyOnce(MatchFlatMap))    process(Filter) else process(FlatMap2)
        case FlatMap2  => if (applyOnce(MatchFlatMap2))   process(Filter) else process(Join)
        case Join      => if (applyOnce(MatchEquiJoin))   process(Filter) else process(Cross)
        case Cross     => if (applyOnce(MatchCross))      process(Filter) else process(Residuals)
        case Residuals => applyOnce(MatchResidual);       process(End)
        case End       => root
      }
      //@formatter:on

      // run the state machine
      process(Start)
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
     * - The matched generator and the matched guard should not have control flow.
     *
     * ==Rewrite==
     * {{{ [[ hd | qs1, x ← filter p xs, qs2, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $p = x => {
     *         $pVals
     *         $pExpr
     *       }
     *       val $ir = $xExpr withFilter $p
     *       $ir
     *     }
     *     $qs2
     *     $qs3
     *     $hd
     *   }
     * }}}
     */
    val MatchFilter: Rule = {
      case (owner, comp @ cs.Comprehension(qs, hd)) => (for {
        // Seq() is to disallow control flow
        grd @ cs.Guard(pred @ core.Let(_, Seq(), _)) <- qs.view
        (qs12, qs3) = splitAt(grd)(qs)
        gen @ cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs12.view
        (qs1, qs2) = splitAt[u.Tree](gen)(qs12)
        refd = api.Tree.refs(grd) intersect gens(qs12)
        if refd.isEmpty || (refd.size == 1 && refd.head == x)
      } yield {
        val (pRef, pVal) = valRefAndDef(owner, "p", core.Lambda(x)(pred))
        val (irRef, irVal) = valRefAndDef(owner, "ir", cs.WithFilter(xExpr)(pRef))
        val vals = Seq.concat(xVals, Seq(pVal, irVal))
        val gen = cs.Generator(x, core.Let(vals: _*)()(irRef))
        val combined = Seq.concat(qs1, Seq(gen), qs2, qs3)
        cs.Comprehension(combined, hd)
      }).headOption.getOrElse(comp)
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
     *     val $y = generator yBlk
     *     $qs3
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The generator of `y` must refer to exactly one generator variable and this variable should be `x`.
     * - The remaining qualifiers, as well as the head should not refer to `x`.
     * - The matched generators should not have control flow.
     *
     * ==Rewrite==
     * {{{ [[ hd | qs1, qs2, y ← flatMap f xs, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     $qs2
     *     val $y = generator {
     *       $xVals
     *       val $f = $fArg => yBlk[fArg/x]
     *       val $ir = $xExpr flatMap $f
     *       $ir
     *     }
     *     $qs3
     *     $hd
     *   }
     * }}}
     */
    val MatchFlatMap: Rule = {
      case (owner, comp @ cs.Comprehension(qs, hd)) => (for {
        // (Seq() is to disallow control flow)
        xGen @ cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view
        (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, yLet @ core.Let(_, Seq(), _)) <- qs23.view
        (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
        if (api.Tree.refs(yGen) intersect gens(qs)) == Set(x)
        if (qs2 ++ qs3 :+ hd).forall(!api.Tree.refs(_).contains(x))
      } yield {
        val (fRef, fVal) = valRefAndDef(owner, "f", core.Lambda(x)(yLet))
        val (irRef, irVal) = valRefAndDef(owner, "ir", cs.FlatMap(xExpr)(fRef))
        val vals = Seq.concat(xVals, Seq(fVal, irVal))
        val gen = cs.Generator(y, core.Let(vals: _*)()(irRef))
        val combined = Seq.concat(qs1, qs2, Seq(gen), qs3)
        cs.Comprehension(combined, hd)
      }).headOption.getOrElse(comp)
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
     * - The generator of `y` must refer to exactly one generator variable and this variable should be `x`.
     * - The matched generators should not have control flow.
     *
     * ==Rewrite==
     * {{{
     *   comprehension {
     *     $qs1
     *     val $xy = generator {
     *       $xVals
     *       val $f = $fArg => {
     *         $yVals[fArg/x]
     *         val $g = $gArg => {
     *           val $ir2 = ($fArg, $gArg)
     *           $ir2
     *         }
     *         val $xy0 = $yExpr[fArg/x] map $g
     *         $xy0
     *       }
     *       val $ir = $xExpr flatMap $f
     *       $ir
     *     }
     *     $qs2[xy._1/x][xy._2/y]  // Note: Introduce ValDefs for xy._1 and xy._2 to maintain ANF
     *     $qs3[xy._1/x][xy._2/y]
     *     $hd[xy._1/x][xy._2/y]
     *   }
     * }}}
     */
    val MatchFlatMap2: Rule = {
      case (owner, comp @ cs.Comprehension(qs, hd)) => (for {
        // (Seq() is to disallow control flow)
        xGen @ cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view
        (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, yLet @ core.Let(yVals, Seq(), yExpr)) <- qs23.view
        (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
        if (api.Tree.refs(yGen) intersect gens(qs)) == Set(x)
      } yield {
        val gArg = api.TermSym.free(api.TermName.fresh(y), Core.bagElemTpe(yExpr))
        val ir2Args = Seq(core.Ref(x), core.Ref(gArg))
        val ir2Rhs = core.DefCall(Some(tuple2))(tuple2App, x.info, gArg.info)(ir2Args)
        val (ir2Ref, ir2Val) = valRefAndDef(owner, "ir2", ir2Rhs)
        val gBody = core.Let(ir2Val)()(ir2Ref)
        val (gRef, gVal) = valRefAndDef(owner, "g", core.Lambda(gArg)(gBody))
        val (xy0Ref, xy0Val) = valRefAndDef(owner, "xy0", cs.Map(yExpr)(gRef))
        val fBody = core.Let(yVals :+ gVal :+ xy0Val: _*)()(xy0Ref)
        val (fRef, fVal) = valRefAndDef(owner, "f", core.Lambda(x)(fBody))
        val (irRef, irVal) = valRefAndDef(owner, "ir", cs.FlatMap(xExpr)(fRef))
        val xyTpe = Core.bagElemTpe(irRef)
        assert(xyTpe.typeConstructor == api.Sym.tuple(2).toTypeConstructor)
        val xy = api.ValSym(owner, api.TermName.fresh("xy"), xyTpe)
        val xyRef = core.Ref(xy)
        val xy1 = core.ValDef(x, core.DefCall(Some(xyRef))(_1)())
        val xy2 = core.ValDef(y, core.DefCall(Some(xyRef))(_2)())
        val bind = prepend(Seq(xy1, xy2)) _
        val vals = Seq.concat(xVals, Seq(fVal, irVal))
        val gen = cs.Generator(xy, core.Let(vals: _*)()(irRef))
        val combined = Seq.concat(qs1, Seq(gen), qs2.view.map(bind), qs3.view.map(bind))
        cs.Comprehension(combined, bind(hd))
      }).headOption.getOrElse(comp)
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
     * - The matched generators should not be correlated, i.e. the generator of `y` should not refer to `x`.
     * - The matched generators should not have control flow.
     *
     * ==Rewrite==
     * {{{ [[ hd[c.x/x][c.y/y] | qs1, c ← ⨯ xs ys, qs3[c.x/x][c.y/y] ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $c = generator {
     *       $xVals
     *       $yVals
     *       val ir = cross($xExpr, $yExpr)
     *       ir
     *     }
     *     $qs3[c.x/x][c.y/y]  // Note: Introduce ValDefs for c.x and c.y to maintain ANF
     *     $hd[c.x/x][c.y/y]
     *   }
     * }}}
     */
    val MatchCross: Rule = {
      case (owner, comp @ cs.Comprehension(qs, hd)) => (for {
        // (Seq() is to disallow control flow)
        xGen @ cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view
        (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, core.Let(yVals, Seq(), yExpr)) <- qs23.view
        (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
        if qs2.isEmpty && !api.Tree.refs(yGen).contains(x)
      } yield {
        val (irRef, irVal) = valRefAndDef(owner, "ir", Combinators.Cross(xExpr, yExpr))
        val cTpe = Core.bagElemTpe(irRef)
        assert(cTpe.typeConstructor == api.Sym.tuple(2).toTypeConstructor)
        val c = api.ValSym(owner, api.TermName.fresh("c"), cTpe)
        val cRef = core.Ref(c)
        val cx = core.ValDef(x, core.DefCall(Some(cRef))(_1)())
        val cy = core.ValDef(y, core.DefCall(Some(cRef))(_2)())
        val bind = prepend(Seq(cx, cy)) _
        val vals = Seq.concat(xVals, yVals, Seq(irVal))
        val gen = cs.Generator(c, core.Let(vals: _*)()(irRef))
        val combined = Seq.concat(qs1, Seq(gen), qs3.view.map(bind))
        cs.Comprehension(combined, bind(hd))
      }).headOption.getOrElse(comp)
    }

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
     *       $joinCondVals0
     *       val jcr = $kxExpr == $kyExpr  // Or `$kyExpr == $kxExpr`
     *       jcr
     *     }
     *     $qs4
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The matched generators should not be correlated, i.e. the generator of `y` should not refer to `x`.
     * - The matched guard should refer to x and y, but not to other generator variables.
     * - $kxExpr's dependencies in $joinCondVals0 should not contain y, and vice versa for $kyExpr
     * - The matched generators and guard should not have control flow.
     *
     * ==Rewrite==
     * {{{ [[ hd[j.x/x][j.y/y] | qs1, j ← ⋈ k₁ k₂ xs ys, qs3[j.x/x][j.y/y], qs4[j.x/x][j.y/y] ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $j = generator {
     *       $xVals
     *       $yVals
     *       val $kx = x => {
     *         $joinCondVals0  // But only those vals that are needed for kxExpr
     *         $kxExpr
     *       }
     *       val $ky = y => {
     *         $joinCondVals0  // But only those vals that are needed for kyExpr
     *         $kyExpr
     *       }
     *       val $ir = join $kx $ky $xExpr $yExpr
     *       $ir
     *     }
     *     $qs3[j.x/x][j.y/y]  // Note: Introduce ValDefs for j.x and j.y to maintain ANF
     *     $qs4[j.x/x][j.y/y]
     *     $hd[j.x/x][j.y/y]
     *   }
     * }}}
     */
    val MatchEquiJoin: Rule = {
      case (owner, comp @ cs.Comprehension(qs, hd)) => (for {
        // (Seq() is to disallow control flow)
        xGen @ cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view
        (qs1, qs234) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, core.Let(yVals, Seq(), yExpr)) <- qs234.view
        (qs2, qs34) = splitAt[u.Tree](yGen)(qs234)
        if qs2.isEmpty && !api.Tree.refs(yGen).contains(x)
        grd @ cs.Guard(core.Let(grdVals, Seq(), core.Ref(jcr))) <- qs34.view
        if (api.Tree.refs(grd) intersect gens(qs)) == Set(x, y)
        jcrVal @ core.ValDef(`jcr`, core.DefCall(Some(eqLhs), eq, _, Seq(eqRhs)), _) <- grdVals
        if eq.name.toString == "$eq$eq"
        joinVals = grdVals.filter(_ != jcrVal)
        (kxExpr, kyExpr) <- Seq((eqLhs, eqRhs), (eqRhs, eqLhs))
        kxBody = Core.dce(core.Let(joinVals: _*)()(kxExpr))
        if !api.Tree.refs(kxBody).contains(y)
        kyBody = Core.dce(core.Let(joinVals: _*)()(kyExpr))
        if !api.Tree.refs(kyBody).contains(x)
        (qs3, qs4) = splitAt[u.Tree](grd)(qs34)
      } yield {
        // It can happen that the key functions have differing return types (e.g. Long and Int).
        // In this case, we add casts before the return expr of the key functions to the weakLub.
        def maybeCast(tree: u.Tree) = if (kxExpr.tpe =:= kyExpr.tpe) tree else tree match {
          case core.Let(valDefs, _, expr) =>
            val asInstanceOf = expr.tpe.member(api.TermName("asInstanceOf")).asTerm
            val kLub = api.Type.weakLub(kxExpr.tpe, kyExpr.tpe)
            val cast = core.DefCall(Some(expr))(asInstanceOf, kLub)()
            val (castRef, castVal) = valRefAndDef(owner, "cast", cast)
            core.Let(valDefs :+ castVal: _*)()(castRef)
          case other => other
        }

        val (kxRef, kxVal) = valRefAndDef(owner, "kx", core.Lambda(x)(maybeCast(kxBody)))
        val (kyRef, kyVal) = valRefAndDef(owner, "ky", core.Lambda(y)(maybeCast(kyBody)))
        val join = Combinators.EquiJoin(kxRef, kyRef)(xExpr, yExpr)
        val (irRef, irVal) = valRefAndDef(owner, "ir", join)
        val jTpe = Core.bagElemTpe(irRef)
        assert(jTpe.typeConstructor == api.Sym.tuple(2).toTypeConstructor)
        val j = api.ValSym(owner, api.TermName.fresh("j"), jTpe)
        val jRef = core.Ref(j)
        val jx = core.ValDef(x, core.DefCall(Some(jRef))(_1)())
        val jy = core.ValDef(y, core.DefCall(Some(jRef))(_2)())
        val bind = prepend(Seq(jx, jy)) _
        val vals = Seq.concat(xVals, yVals, Seq(kxVal, kyVal, irVal))
        val gen = cs.Generator(j, core.Let(vals: _*)()(irRef))
        val combined = Seq.concat(qs1, Seq(gen), qs3.view.map(bind), qs4.view.map(bind))
        cs.Comprehension(combined, bind(hd))
      }).headOption.getOrElse(comp)
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
     * ==Guard==
     * - The generator should not have control flow.
     *
     * ==Rewrite==
     * {{{ map hd xs }}}
     * {{{
     *   $xVals
     *   val $f = $fArg => $hd[fArg/x]
     *   val $ir = $xExpr map $f
     *   $ir
     * }}}
     */
    val MatchResidual: Rule = {
      case (own, cs.Comprehension(Seq(cs.Generator(x, core.Let(vs, Seq(), expr))), cs.Head(hd))) =>
        val (fRef, fVal) = valRefAndDef(own, "f", core.Lambda(x)(hd))
        val (irRef, irVal) = valRefAndDef(own, "ir", cs.Map(expr)(fRef))
        core.Let(vs ++ Seq(fVal, irVal): _*)()(irRef)
    }

    private def prepend(vals: Seq[u.ValDef])(in: u.Tree): u.Tree = {
      val refs = api.Tree.refs(in)
      val prefix = vals.filter(v => refs(v.symbol.asTerm))
      def prepend(let: u.Tree): u.Block = let match {
        case core.Let(suffix, defs, expr) =>
          core.Let(prefix ++ suffix: _*)(defs: _*)(expr)
        case expr =>
          core.Let(prefix: _*)()(expr)
      }

      in match {
        case cs.Generator(x, gen) => cs.Generator(x, prepend(gen))
        case cs.Guard(pred) => cs.Guard(prepend(pred))
        case cs.Head(expr) => cs.Head(prepend(expr))
        case tree => prepend(tree)
      }
    }

    /** Creates a ValDef, and returns its Ident on the left hand side. */
    private def valRefAndDef(own: u.Symbol, name: String, rhs: u.Tree): (u.Ident, u.ValDef) = {
      val lhs = api.ValSym(own, api.TermName.fresh(name), rhs.tpe)
      (core.Ref(lhs), core.ValDef(lhs, rhs))
    }

    private def gens(qs: Seq[u.Tree]): Set[u.TermSymbol] =
      qs.collect { case cs.Generator(x, _) => x } (breakOut)
  }
}
