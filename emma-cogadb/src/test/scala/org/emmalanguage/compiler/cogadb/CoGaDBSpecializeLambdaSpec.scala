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
package compiler.cogadb

import compiler.BaseCompilerSpec
import compiler.lang.cogadb.ast
import org.emmalanguage.api.CoGaDBTable
import org.emmalanguage.api.cogadb.CoGaDBNtv
import test.schema.Literature._

import scala.collection.breakOut


class CoGaDBSpecializeLambdaSpec extends BaseCompilerSpec with CoGaDBSpec {

  import compiler._
  import Core.{Lang => core}

  lazy val actPipeline = ??? //testp
  lazy val expPipeline = ??? //anfp
  private lazy val testPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(Core.dscf, collectFirstLambda, testTransform).compose(_.tree)

  private lazy val dscfPipeline: u.Expr[Any] => u.Tree =
    pipeline(true, withPost = false)(Core.dscf).compose(_.tree)

  lazy val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(true)(Core.anf, collectFirstLambda).compose(_.tree)

  private lazy val testTransform: u.Tree => u.Tree = {
    case lambda@core.Lambda(_, Seq(core.ParDef(p, _)), core.Let(vals, Seq(), core.Ref(r))) =>

      val valOf = vals.map(vd => {
        vd.symbol.asTerm -> vd.rhs
      })(breakOut): Map[u.TermSymbol, u.Tree]

      def isProductApply(x: u.TermSymbol): Boolean = valOf.get(x) match {
        case Some(core.DefCall(Some(core.Ref(t)), method, _, Seq(args))) if method.isSynthetic
          && method.name == api.TermName.app
          && t.companion.info.baseClasses.contains(api.Sym[Product]) => true
        case _ => false
      }

      //println(valOf)
      val tgt = api.Sym[ast.AttrRef.type].asModule
      val app = tgt.info.member(api.TermName.app).asMethod

      if (valOf.contains(r)) {
        val mapSym = Map(
          p -> api.TermSym.free(p.name, api.Type[String])
        ) ++ (vals.map(vd => {
          val x = vd.symbol.asTerm
          //println(x)

          val W =
            if (x == r && isProductApply(x)) api.Type.kind1[Seq](api.Type[ast.AttrRef])
            else api.Type[ast.AttrRef]

          val w = api.TermSym.free(x.name, W)
          x -> w
        })(breakOut): Map[u.TermSymbol, u.TermSymbol])

        //println(mapSym)
        val mapArgs = (args: Seq[u.Tree]) => args map {
          case core.Ref(z) if mapSym contains z => core.Ref(mapSym(z))
          case arg => arg
        }

        val vals1 = for (core.ValDef(x, rhs) <- vals) yield rhs match {
          // translate projections
          case core.DefCall(Some(core.Ref(z)), method, Seq(), Seq())
            if method.isGetter =>

            //println(test)
            mapSym.get(z).map(w => {

              val ags = Seq(core.Ref(w), core.Lit(method.name.toString), core.Lit(method.name.toString), core.Lit(1))
              val rhs = core.DefCall(Some(core.Ref(tgt)), app, Seq(), Seq(ags))
              core.ValDef(mapSym(x), rhs)
            })

          // translate case class constructors in return position
          case core.DefCall(_, app2, _, Seq(args))
            if x == r && isProductApply(x) =>

            val tgt1 = api.Sym[Seq.type].asModule
            val app1 = tgt1.info.member(api.TermName.app).asMethod

            val as1 = app.paramLists.head.map(p =>
              core.Lit(p.name.toString)
            )
            val as2 = args.map({
              case core.Ref(a) if mapSym contains a => core.Ref(mapSym(a))
              case a => a
            })
            val Tpe = api.Type.kind1[Seq](api.Sym[ast.AttrRef].tpe)
            val lhs = mapSym(r)

            val argss = Seq(as2)
            val rhs = core.DefCall(Some(core.Ref(tgt1)), app1, Seq(api.Type[ast.AttrRef]), argss)
            Some(core.ValDef(lhs, rhs))

          case _ => None
        }

        val (vals2, expr1) = if (isProductApply(r)) {
          val vs2 = Seq.empty[Option[u.ValDef]]
          val ex2 = core.Ref(mapSym(r))

          (vs2, ex2)
        } else {
          val tgt = api.Sym[Seq.type].asModule
          val app = tgt.info.member(api.TermName.app).asMethod

          val as2 = Seq(core.Ref(mapSym(r)))

          val agss = Seq(as2)
          val rhs = core.DefCall(Some(core.Ref(tgt)), app, Seq(api.Type[ast.AttrRef]), agss)
          val Tpe = api.Type.kind1[Seq](api.Type[ast.AttrRef])
          val lhs = api.TermSym.free(api.TermName.fresh("r"), Tpe)

          val vs2 = Seq(Some(core.ValDef(lhs, rhs)))
          val ex2 = core.Ref(lhs)

          (vs2, ex2)
        }


        //vals1.foreach(println)
        val ret = core.Lambda(Seq(mapSym(p)), core.Let((vals1 ++ vals2).flatten, Seq(), expr1))
        ret
      } else lambda

  }

  lazy val collectFirstLambda: u.Tree => u.Tree = tree => (tree collect {
    case t: u.Function => t
  }).head

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

  "playground #1" in {

    val exp = dscfPipeline(u.reify {
      val a1 = ast.AttrRef("ORDERS", "O_ORDERKEY", "O_ORDERKEY", 1)
      a1
    })

    val act = {
      val a1 = {
        val tgt = api.Sym[ast.AttrRef.type].asModule
        val app = tgt.info.member(api.TermName.app).asMethod
        val rhs = core.DefCall(Some(core.Ref(tgt)), app, Seq(), Seq(Seq(
          core.Lit("ORDERS"),
          core.Lit("O_ORDERKEY"),
          core.Lit("O_ORDERKEY"),
          core.Lit(1))))

        val lhs = api.TermSym(api.Owner.encl, api.TermName("a1"), rhs.tpe)

        lhs -> rhs
      }

      val expr = refOf(a1)

      core.Let(Seq(defOf(a1)), Seq(), expr)
    }
    act shouldBe alphaEqTo(exp)
  }

  "playground #2" in {
    val act = {
      val a1 = {
        val tgt = api.Sym[ast.AttrRef.type].asModule
        val app = tgt.info.member(api.TermName.app).asMethod
        val rhs = core.DefCall(Some(core.Ref(tgt)), app, Seq(), Seq(Seq(
          core.Lit("ORDERS"),
          core.Lit("O_ORDERKEY"),
          core.Lit("O_ORDERKEY"),
          core.Lit(1))))

        val lhs = api.TermSym(api.Owner.encl, api.TermName("a1"), rhs.tpe)

        lhs -> rhs
      }

      val a2 = {
        val tgt = api.Sym[ast.AttrRef.type].asModule
        val app = tgt.info.member(api.TermName.app).asMethod
        val rhs = core.DefCall(Some(core.Ref(tgt)), app, Seq(), Seq(Seq(
          core.Lit("LINEITEM"),
          core.Lit("L_ORDERKEY"),
          core.Lit("L_ORDERKEY"),
          core.Lit(1))))

        val lhs = api.TermSym(api.Owner.encl, api.TermName("a2"), rhs.tpe)

        lhs -> rhs
      }


      val x1 = {
        val tgt = api.Sym[ast.ColCol.type].asModule
        val app = tgt.info.member(api.TermName.app).asMethod

        val tgt1 = api.Sym[ast.Equal.type].asModule
        //val app1 = tgt1.info.member(api.TermName.app).asMethod


        val rhs = core.DefCall(Some(core.Ref(tgt)), app, Seq(), Seq(Seq(
          core.ValRef(a1._1),
          core.ValRef(a2._1),
          core.Ref(tgt1)
        )))

        val lhs = api.TermSym(api.Owner.encl, api.TermName("x1"), rhs.tpe)

        lhs -> rhs
      }

      val expr = refOf(x1)

      core.Let(Seq(defOf(a1), defOf(a2), defOf(x1)), Seq(), expr)
    }

    val exp = dscfPipeline(u.reify {
      val a1 = ast.AttrRef("ORDERS", "O_ORDERKEY", "O_ORDERKEY", 1)
      val a2 = ast.AttrRef("LINEITEM", "L_ORDERKEY", "L_ORDERKEY", 1)
      val x1 = ast.ColCol(a1, a2, ast.Equal)
      x1
    })

    act shouldBe alphaEqTo(exp)
  }
  "playground #3" in {

    val act = testPipeline(u.reify {
      (p: Book) => {
        val y = p.title
        y
      }

    })

    val exp = anfPipeline(u.reify {
      (p: String) => {
        val y = ast.AttrRef(p, "title", "title", 1)
        val r = Seq(y)
        r
      }

    })

    act shouldBe alphaEqTo(exp)
  }
  "playground #4" in {

    val act = testPipeline(u.reify {
      (p: Book) => {
        val y = p.title
        val z = p.author
        val r = (y, z)
        r
      }
    })

    val exp = anfPipeline(u.reify {
      (p: String) => {
        val y = ast.AttrRef(p, "title", "title", 1)
        val z = ast.AttrRef(p, "author", "author", 1)
        val r = Seq(y, z)
        r
      }
    })

    act shouldBe alphaEqTo(exp)
  }
  "playground #5" in withCoGaDB(implicit cogadb =>  {

    val act = idPipeline(u.reify{
      val s1 = (1, 2)
      val s2 = (2, 3)
      val s3 = (3, 4)
      val x1 = Seq(s1, s2, s3)
      val xs = CoGaDBTable.apply(x1)
      val fn = (x: (Int, Int)) => {
        val x2 = x._1
        x2
      }
      val ys = xs.map(fn)
    })

    val exp = testPipeline(u.reify{
      val xs = CoGaDBTable.apply(Seq((1, 2), (2, 3), (3, 4)))
      val fn = (x: String) => {
        val y = ast.AttrRef(x, "title", "title", 1)
        val z = Seq(y)
        z
      }
      val ys = CoGaDBNtv.project[(Int,Int),Int](fn)(xs)
    })

    act shouldBe alphaEqTo(exp)

  })
}