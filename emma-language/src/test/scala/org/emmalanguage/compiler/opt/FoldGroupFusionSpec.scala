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

import api._
import api.backend.LocalOps
import compiler.BaseCompilerSpec

class FoldGroupFusionSpec extends BaseCompilerSpec {

  import compiler._

  val testPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.lift,
      FoldGroupFusion.foldGroupFusion.timed
    ) compose (_.tree)

  lazy val liftPipeline: u.Expr[Any] => u.Tree = {
    pipeline(typeCheck = true)(Core.lift)
  } compose (_.tree)

  val us = DataBag((1 to 100).map(u => (u, u * u, u * u * u)))
  val _1 = (x: (Int, Int, Int)) => x._1
  val _2 = (x: (Int, Int, Int)) => x._2

  "applies on folds" - {
    "with a static algebra" in {
      val act = testPipeline(u.reify {
        val _1: this._1.type = this._1
        val _2: this._2.type = this._2
        val us: this.us.type = this.us

        val gr1 = us.groupBy(_1)
        val rs1 = for (g <- gr1) yield {
          val key1 = g.key
          val vls1 = g.values
          val agg1 = vls1.fold(alg.Size)
          val res1 = (key1, agg1)
          res1
        }

        val gr2 = us.groupBy(_2)
        val rs2 = for (g <- gr2) yield {
          val key2 = g.key
          val vls2 = g.values
          val agg2 = vls2.fold(alg.Size)
          val res2 = (key2, agg2)
          res2
        }

        val rs = (rs1, rs2)
        rs
      })

      val exp = liftPipeline(u.reify {
        val _1: this._1.type = this._1
        val _2: this._2.type = this._2
        val us: this.us.type = this.us

        val gr1 = LocalOps.foldGroup(us, _1, alg.Size)
        val rs1 = for (g <- gr1) yield {
          val key1 = g.key
          val agg1 = g.values
          val res1 = (key1, agg1)
          res1
        }

        val gr2 = LocalOps.foldGroup(us, _2, alg.Size)
        val rs2 = for (g <- gr2) yield {
          val key2 = g.key
          val agg2 = g.values
          val res2 = (key2, agg2)
          res2
        }

        val rs = (rs1, rs2)
        rs
      })

      act shouldBe alphaEqTo(exp)
    }

    "with independent dynamic algebras" in {
      val act = testPipeline(u.reify {
        val or = Ordering.Int
        val _1: this._1.type = this._1
        val _2: this._2.type = this._2
        val us: this.us.type = this.us

        val gr1 = us.groupBy(_1)
        val pj1 = (x: (Int, Int, Int)) => x._1
        val pj2 = (x: (Int, Int, Int)) => x._2
        val pj3 = (x: (Int, Int, Int)) => x._3
        val rs1 = for (g <- gr1) yield {
          val al11 = alg.Min(or)
          val al12 = alg.Min(or)
          val al13 = alg.Min(or)
          val al14 = alg.Map(pj1, al11)
          val al15 = alg.Map(pj2, al12)
          val al16 = alg.Map(pj3, al13)
          val al17 = alg.Alg3(al14, al15, al16)

          val key1 = g.key
          val vls1 = g.values
          val agg1 = vls1.fold(al17)
          val gt11 = agg1._1
          val gt12 = agg1._2
          val gt13 = agg1._3
          val res1 = (key1, gt11, gt12, gt13)
          res1
        }

        val gr2 = us.groupBy(_2)
        val rs2 = for (g <- gr2) yield {
          val al21 = alg.Max(or)
          val al22 = alg.Max(or)
          val al23 = alg.Map(pj1, al21)
          val al24 = alg.Map(pj2, al22)
          val al25 = alg.Alg2(al23, al24)

          val key2 = g.key
          val vls2 = g.values
          val agg3 = vls2.fold(al25)
          val gt21 = agg3._1
          val gt22 = agg3._2
          val res2 = (key2, gt21, gt22)
          res2
        }

        val rs = (rs1, rs2)
        rs
      })

      val exp = liftPipeline(u.reify {
        val or = Ordering.Int
        val _1: this._1.type = this._1
        val _2: this._2.type = this._2
        val us: this.us.type = this.us

        val pj1 = (x: (Int, Int, Int)) => x._1
        val pj2 = (x: (Int, Int, Int)) => x._2
        val pj3 = (x: (Int, Int, Int)) => x._3
        val al11 = alg.Min(or)
        val al12 = alg.Min(or)
        val al13 = alg.Min(or)
        val al14 = alg.Map(pj1, al11)
        val al15 = alg.Map(pj2, al12)
        val al16 = alg.Map(pj3, al13)
        val al17 = alg.Alg3(al14, al15, al16)
        val gr1 = LocalOps.foldGroup(us, _1, al17)
        val rs1 = for (g <- gr1) yield {
          val key1 = g.key
          val agg1 = g.values
          val gt11 = agg1._1
          val gt12 = agg1._2
          val gt13 = agg1._3
          val res1 = (key1, gt11, gt12, gt13)
          res1
        }

        val al21 = alg.Max(or)
        val al22 = alg.Max(or)
        val al23 = alg.Map(pj1, al21)
        val al24 = alg.Map(pj2, al22)
        val al25 = alg.Alg2(al23, al24)
        val gr2 = LocalOps.foldGroup(us, _2, al25)
        val rs2 = for (g <- gr2) yield {
          val key2 = g.key
          val agg3 = g.values
          val gt21 = agg3._1
          val gt22 = agg3._2
          val res2 = (key2, gt21, gt22)
          res2
        }

        val rs = (rs1, rs2)
        rs
      })

      act shouldBe alphaEqTo(exp)
    }
  }

  "does not apply on folds depending on the group value" in {
    val act = testPipeline(u.reify {
      val or = Ordering.Int
      val _1: this._1.type = this._1
      val _2: this._2.type = this._2
      val us: this.us.type = this.us

      val pj1 = (x: (Int, Int, Int)) => x._1
      val pj2 = (x: (Int, Int, Int)) => x._2
      val pj3 = (x: (Int, Int, Int)) => x._3

      val gr1 = us.groupBy(_1)
      val rs1 = for (g <- gr1) yield {
        val key1 = g.key
        val vls1 = g.values

        val al11 = alg.Top(key1, or)
        val al12 = alg.Min(or)
        val al13 = alg.Min(or)
        val al14 = alg.Map(pj1, al11)
        val al15 = alg.Map(pj2, al12)
        val al16 = alg.Map(pj3, al13)
        val al17 = alg.Alg3(al14, al15, al16)

        val agg1 = vls1.fold(al17)
        val gt11 = agg1._1
        val gt12 = agg1._2
        val gt13 = agg1._3
        val res1 = (key1, gt11, gt12, gt13)
        res1
      }

      val gr2 = us.groupBy(_2)
      val rs2 = for (g <- gr2) yield {
        val al21 = alg.Max(or)
        val al22 = alg.Max(or)
        val al23 = alg.Map(pj1, al21)
        val al24 = alg.Map(pj2, al22)
        val al25 = alg.Alg2(al23, al24)

        val key2 = g.key
        val vls2 = g.values
        val agg3 = vls2.fold(al25)
        val gt21 = agg3._1
        val gt22 = agg3._2
        val res2 = (key2, gt21, gt22)
        res2
      }

      val rs = (rs1, rs2)
      rs
    })

    val exp = liftPipeline(u.reify {
      val or = Ordering.Int
      val _1: this._1.type = this._1
      val _2: this._2.type = this._2
      val us: this.us.type = this.us

      val pj1 = (x: (Int, Int, Int)) => x._1
      val pj2 = (x: (Int, Int, Int)) => x._2
      val pj3 = (x: (Int, Int, Int)) => x._3

      val gr1 = us.groupBy(_1)
      val rs1 = for (g <- gr1) yield {
        val key1 = g.key
        val vls1 = g.values

        val al11 = alg.Top(key1, or)
        val al12 = alg.Min(or)
        val al13 = alg.Min(or)
        val al14 = alg.Map(pj1, al11)
        val al15 = alg.Map(pj2, al12)
        val al16 = alg.Map(pj3, al13)
        val al17 = alg.Alg3(al14, al15, al16)

        val agg1 = vls1.fold(al17)
        val gt11 = agg1._1
        val gt12 = agg1._2
        val gt13 = agg1._3
        val res1 = (key1, gt11, gt12, gt13)
        res1
      }

      val al21 = alg.Max(or)
      val al22 = alg.Max(or)
      val al23 = alg.Map(pj1, al21)
      val al24 = alg.Map(pj2, al22)
      val al25 = alg.Alg2(al23, al24)
      val gr2 = LocalOps.foldGroup(us, _2, al25)
      val rs2 = for (g <- gr2) yield {
        val key2 = g.key
        val agg3 = g.values
        val gt21 = agg3._1
        val gt22 = agg3._2
        val res2 = (key2, gt21, gt22)
        res2
      }

      val rs = (rs1, rs2)
      rs
    })

    act shouldBe alphaEqTo(exp)
  }
}
