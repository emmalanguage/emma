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

import scala.annotation.tailrec

private[comprehension] trait Combination extends Common {
  self: Core with Comprehension =>

  import Comprehension.{splitAt,Combinators}
  import Core.{Lang => core}
  import UniverseImplicits._

  private[comprehension] object Combination {

    private val cs = new Comprehension.Syntax(API.bagSymbol)

    //@formatter:off
    private val tuple2   = api.Type[(Nothing, Nothing)]
    private val tuple2_1 = tuple2.member(api.TermName("_1")).asTerm
    private val tuple2_2 = tuple2.member(api.TermName("_2")).asTerm
    //@formatter:on

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
    lazy val transform: u.Tree => u.Tree = root =>
      api.TopDown.transform {
        case comp@cs.Comprehension(_,_) => combine(comp)
      }(root).tree

    /**
     * Performs the combination for one comprehension. That is, the root of the given tree
     * must be a comprehension, and it eliminates only this one.
     *
     * Note: We can't just call transform with the Match... rules, because then the application
     * of the rules would be interleaved across the outer and the nested comprehensions, which
     * would mess up the order of rule applications for a single comprehension.
     */
    lazy val combine: u.Tree => u.Tree = root0 => {

      var root = root0

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

      def applyOnce(rule: u.Tree =?> u.Tree): Boolean = {
        if (rule.isDefinedAt(root)) {
          val prevRoot = root
          root = rule(root)
          root != prevRoot
        } else {
          false
        }
      }

      @tailrec
      def applyExhaustively(rule: u.Tree =?> u.Tree): Unit = {
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
    val MatchFilter: u.Tree =?> u.Tree = Function.unlift((tree: u.Tree) => tree match {
      case cs.Comprehension(qs, hd) =>

        val allGenVars = (for {
          cs.Generator(x, _) <- qs
        } yield x).toSet

        (for {
          grd@cs.Guard(grdBlk@core.Let(pVals, Seq(), pExpr)) <- qs.view // (Seq() is to disallow control flow)
          (qs12, qs3) = splitAt(grd)(qs)
          gen@cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs12
          (qs1, qs2) = splitAt[u.Tree](gen)(qs12)
          if {
            val refdGens = api.Tree.refs(grd) intersect allGenVars
            refdGens.isEmpty || (refdGens.size == 1 && refdGens.head == x)
          }
        } yield {

          val pArg = api.TermSym.free(api.TermName.fresh("pArg"), Core.bagElemTpe(xExpr))
          val pBdy = replaceRefs(x, pArg)(grdBlk)
          val (pRef, pVal) = valDefAndRef("p", core.Lambda(pArg)(pBdy))
          val (irRef, irVal) = valDefAndRef("ir", cs.WithFilter(xExpr)(pRef))

          //@formatter:off
          cs.Comprehension(
            qs1 ++
            Seq(cs.Generator(x, core.Let(
              xVals :+ pVal :+ irVal: _*
            )()(irRef))) ++
            qs2 ++ qs3,
            hd
          )
          //@formatter:on

        }).headOption

      case _ => None
    })


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
    val MatchFlatMap: u.Tree =?> u.Tree = Function.unlift((tree: u.Tree) => tree match {
      case cs.Comprehension(qs, hd) =>

        val allGenVars = (for {
          cs.Generator(x, _) <- qs
        } yield x).toSet

        (for {
          xGen@cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view // (Seq() is to disallow control flow)
          (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
          yGen@cs.Generator(y, yBlk@core.Let(_, Seq(), _)) <- qs23
          (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
          if (api.Tree.refs(yGen) intersect allGenVars) == Set(x)
          if (qs2 ++ qs3 :+ hd).forall(!api.Tree.refs(_).contains(x))
        } yield {

          val fArg = api.TermSym.free(api.TermName.fresh("fArg"), Core.bagElemTpe(xExpr))
          val fBdy = replaceRefs(x, fArg)(yBlk)
          val (fRef, fVal) = valDefAndRef("f", core.Lambda(fArg)(fBdy))
          val (irRef, irVal) = valDefAndRef("ir", cs.FlatMap(xExpr)(fRef))

          //@formatter:off
          cs.Comprehension(
            qs1 ++ qs2 ++
            Seq(cs.Generator(y, core.Let(
              xVals :+ fVal :+ irVal: _*
            )()(irRef))) ++
            qs3,
            hd
          )
          //@formatter:on

        }).headOption

      case _ =>
        None
    })

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
    val MatchFlatMap2: u.Tree =?> u.Tree = Function.unlift((tree: u.Tree) => tree match {
      case cs.Comprehension(qs, hd) =>

        val allGenVars = (for {
          cs.Generator(x, _) <- qs
        } yield x).toSet

        (for {
          xGen@cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view // (Seq() is to disallow control flow)
          (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
          yGen@cs.Generator(y, yBlk@core.Let(yVals, Seq(), yExpr)) <- qs23
          (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
          if (api.Tree.refs(yGen) intersect allGenVars) == Set(x)
        } yield {

          val fArg = api.TermSym.free(api.TermName.fresh("fArg"), Core.bagElemTpe(xExpr))
          val fArgRef = core.Ref(fArg)
          val xTofArg = (tree: u.Tree) => replaceRefs(x, fArg)(tree)
          val yValsRepl = yVals map { case core.ValDef(lhs, rhs, fs) => core.ValDef(lhs, xTofArg(rhs), fs) }
          val yExprRepl = xTofArg(yExpr)
          val gArg = api.TermSym.free(api.TermName.fresh("gArg"), Core.bagElemTpe(yExpr))
          val gArgRef = core.Ref(gArg)
          val (ir2Ref, ir2Val) = valDefAndRef("ir2", core.Inst(tuple2, fArgRef.tpe, gArgRef.tpe)(
            Seq(fArgRef, gArgRef)))
          val gBdy = core.Let(ir2Val)()(ir2Ref)
          val (gRef, gVal) = valDefAndRef("g", core.Lambda(gArg)(gBdy))
          val (xy0Ref, xy0Val) = valDefAndRef("xy0", cs.Map(yExprRepl)(gRef))
          val fBdy = core.Let(yValsRepl :+ gVal :+ xy0Val: _*)()(xy0Ref)
          val (fRef, fVal) = valDefAndRef("f", core.Lambda(fArg)(fBdy))
          val (irRef, irVal) = valDefAndRef("ir", cs.FlatMap(xExpr)(fRef))

          //@formatter:off
          val xyTpe = Core.bagElemTpe(irRef)
          assert(xyTpe.typeConstructor == api.Sym.tuple(2).toTypeConstructor)
          val xySym = api.TermSym.free(api.TermName.fresh("xy"), xyTpe)
          val xyRef = core.Ref(xySym)
          val xy_1  = core.DefCall(Some(xyRef))(tuple2_1)()
          val xy_2  = core.DefCall(Some(xyRef))(tuple2_2)()

          val replaceXYandANF =
            api.Tree.replace(core.Ref(x), xy_1) andThen
            api.Tree.replace(core.Ref(y), xy_2) andThen
            Core.anf

          val qsReplFunc = this.qsReplFunc(replaceXYandANF)
          val qs2Repl = qs2 map qsReplFunc
          val qs3Repl = qs3 map qsReplFunc
          val hdRepl = cs.Head(Comprehension.asLet(replaceXYandANF(cs.Head.unapply(hd).get)))

          cs.Comprehension(
            qs1 ++
            Seq(cs.Generator(xySym, core.Let(
              xVals :+ fVal :+ irVal: _*
            )()(irRef))) ++
            qs2Repl ++ qs3Repl,
            hdRepl
          )
          //@formatter:on

        }).headOption

      case _ =>
        None
    })

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
    val MatchCross: u.Tree =?> u.Tree = Function.unlift((tree: u.Tree) => tree match {
      case cs.Comprehension(qs, hd) =>

        (for {
          xGen@cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view // (Seq() is to disallow control flow)
          (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
          yGen@cs.Generator(y, core.Let(yVals, Seq(), yExpr)) <- qs23
          (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
          if qs2.isEmpty
          if !api.Tree.refs(yGen).contains(x)
        } yield {

          val (irRef, irVal) = valDefAndRef("ir", Combinators.Cross(xExpr, yExpr))

          //@formatter:off
          val cTpe = Core.bagElemTpe(irRef)
          assert(cTpe.typeConstructor == api.Sym.tuple(2).toTypeConstructor)
          val cSym = api.TermSym.free(api.TermName.fresh("c"), cTpe)
          val cRef = core.Ref(cSym)
          val cx   = core.DefCall(Some(cRef))(tuple2_1)()
          val cy   = core.DefCall(Some(cRef))(tuple2_2)()

          val replaceXYandANF =
            api.Tree.replace(core.Ref(x), cx) andThen
            api.Tree.replace(core.Ref(y), cy) andThen
            Core.anf

          val qs3Repl = qs3 map qsReplFunc(replaceXYandANF)
          val hdRepl = cs.Head(Comprehension.asLet(replaceXYandANF(cs.Head.unapply(hd).get)))

          cs.Comprehension(
            qs1 ++
            Seq(cs.Generator(cSym, core.Let(
              xVals ++ yVals :+ irVal: _*
            )()(irRef))) ++
            qs3Repl,
            hdRepl
          )
          //@formatter:on

        }).headOption

      case _ =>
        None
    })

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
    val MatchEquiJoin: u.Tree =?> u.Tree = Function.unlift((tree: u.Tree) => tree match {
      case cs.Comprehension(qs, hd) =>

        val allGenVars = (for {
          cs.Generator(x, _) <- qs
        } yield x).toSet

        (for {
          xGen@cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view // (Seq() is to disallow control flow)
          (qs1, qs234) = splitAt[u.Tree](xGen)(qs)
          yGen@cs.Generator(y, core.Let(yVals, Seq(), yExpr)) <- qs234
          (qs2, qs34) = splitAt[u.Tree](yGen)(qs234)
          if qs2.isEmpty
          if !api.Tree.refs(yGen).contains(x)
          grd@cs.Guard(grdBlk@core.Let(joinCondVals, Seq(), joinCondExpr@core.Ref(jcr))) <- qs34
          if (api.Tree.refs(grd) intersect allGenVars) == Set(x, y)
          jcrVal@core.ValDef(`jcr`, core.DefCall(Some(eqeqLhs), method, _, Seq(eqeqRhs)), _) <- joinCondVals
          if method.name.toString == "$eq$eq"
          joinCondVals0 = joinCondVals.filter(_ != jcrVal)
          (kxExpr, kyExpr) <- Seq((eqeqLhs, eqeqRhs), (eqeqRhs, eqeqLhs))
          kxBdy0 = Core.dce(core.Let(joinCondVals0: _*)()(kxExpr))
          if !api.Tree.refs(kxBdy0).contains(y)
          kyBdy0 = Core.dce(core.Let(joinCondVals0: _*)()(kyExpr))
          if !api.Tree.refs(kyBdy0).contains(x)
          (qs3, qs4) = splitAt[u.Tree](grd)(qs34)
        } yield {

          // It can happen that the key functions have differing return types (e.g. Long and Int).
          // In this case, we add casts before the return expression of the key functions to the weakLub.
          val maybeAddCast = (t: u.Tree) => if (kxExpr.tpe == kyExpr.tpe) t
          else t match {
            case core.Let(valDefs, _, expr) =>
              val asInstanceOf = expr.tpe.member(api.TermName("asInstanceOf")).asTerm
              val kLub = api.Type.weakLub(kxExpr.tpe, kyExpr.tpe)
              val (castRef, castVal) = valDefAndRef("cast", core.DefCall(Some(expr))(asInstanceOf, kLub)())
              core.Let(valDefs :+ castVal: _*)()(castRef)
          }

          val kxArg = api.TermSym.free(api.TermName.fresh("kxArg"), Core.bagElemTpe(xExpr))
          val kxBdy = replaceRefs(x, kxArg)(maybeAddCast(kxBdy0))
          val (kxRef, kxVal) = valDefAndRef("kx", core.Lambda(kxArg)(kxBdy))

          val kyArg = api.TermSym.free(api.TermName.fresh("kyArg"), Core.bagElemTpe(yExpr))
          val kyBdy = api.TopDown.transform { case core.Ref(`y`) => core.Ref(kyArg) }(
            api.Tree.refreshAll(maybeAddCast(kyBdy0))).tree
          val (kyRef, kyVal) = valDefAndRef("ky", core.Lambda(kyArg)(kyBdy))

          val (irRef, irVal) = valDefAndRef("ir", Combinators.EquiJoin(kxRef, kyRef)(xExpr, yExpr))

          //@formatter:off
          val jTpe = Core.bagElemTpe(irRef)
          assert(jTpe.typeConstructor == api.Sym.tuple(2).toTypeConstructor)
          val jSym = api.TermSym.free(api.TermName.fresh("j"), jTpe)
          val jRef = core.Ref(jSym)
          val jx   = core.DefCall(Some(jRef))(tuple2_1)()
          val jy   = core.DefCall(Some(jRef))(tuple2_2)()

          val replaceXYandANF =
            api.Tree.replace(core.Ref(x), jx) andThen
            api.Tree.replace(core.Ref(y), jy) andThen
            Core.anf

          val qsReplFunc = this.qsReplFunc(replaceXYandANF)
          val qs3Repl = qs3 map qsReplFunc
          val qs4Repl = qs4 map qsReplFunc
          val hdRepl  = cs.Head(Comprehension.asLet(replaceXYandANF(cs.Head.unapply(hd).get)))

          cs.Comprehension(
            qs1 ++
            Seq(cs.Generator(jSym, core.Let(
              xVals ++ yVals :+ kxVal :+ kyVal :+ irVal: _*
            )()(irRef))) ++
            qs3Repl ++ qs4Repl,
            hdRepl
          )
          //@formatter:on

        }).headOption

      case _ =>
        None
    })

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
    val MatchResidual: u.Tree =?> u.Tree = Function.unlift((tree: u.Tree) => tree match {
      case cs.Comprehension(Seq(cs.Generator(x, core.Let(xVals, Seq(), xExpr))), cs.Head(hd)) =>

        val fArg = api.TermSym.free(api.TermName.fresh("fArg"), Core.bagElemTpe(xExpr))
        val (fRef, fVal) = valDefAndRef("f", core.Lambda(fArg)(replaceRefs(x, fArg)(hd)))
        val (irRef, irVal) = valDefAndRef("ir", cs.Map(xExpr)(fRef))

        Some(core.Let(xVals :+ fVal :+ irVal :_*)()(irRef))

      case _ =>
        None
    })

    private def replaceRefs(find: u.TermSymbol, repl: u.TermSymbol)(t: u.Tree): u.Tree = {
      api.Tree.replace(core.Ref(find), core.Ref(repl))(t)
    }

    private def qsReplFunc(f: u.Tree => u.Tree): u.Tree =?> u.Tree = {
      case cs.Generator(sym, blk) => cs.Generator(sym, Comprehension.asLet(f(blk)))
      case cs.Guard(blk) => cs.Guard(Comprehension.asLet(f(blk)))
    }

    /** Creates a ValDef, and returns its Ident on the left hand side. */
    private def valDefAndRef(name: String, rhs: u.Tree): (u.Ident, u.ValDef) = {
      val sym = api.TermSym.free(api.TermName.fresh(name), rhs.tpe)
      (core.Ref(sym), api.ValDef(sym, rhs))
    }
  }

}
