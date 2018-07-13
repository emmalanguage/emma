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
package compiler

import api.DataBag
import api.backend.LocalOps._
import api._
import api.alg.Count
import labyrinth._
import labyrinth.operators._
import labyrinth.partitioners._
import ir.DSCFAnnotations.loopBody
import ir.DSCFAnnotations.suffix
import ir.DSCFAnnotations.whileLoop

import org.apache.flink.core.fs.FileInputSplit

import java.util.UUID


class TestInt(var v: Int) {
  def addd(u: Int, w: Int, x: Int)(m: Int, n: Int)(s: Int, t: Int) : Int =
    this.v + u + w + x + m + n + s + t

  def add1() : Unit = { v = v + 1 }
}

class LabyrinthCompilerSpec extends BaseCompilerSpec
  with LabyrinthCompilerAware
  with LabyrinthAware {

  //implicit val env = defaultFlinkStreamEnv

  case class Config
  (
    // general parameters
    command     : Option[String]       = None,
    // union of all parameters bound by a command option or argument
    // (in alphabetic order)
    csv         : CSV                  = CSV(),
    epsilon     : Double               = 0,
    iterations  : Int                  = 0,
    input       : String               = System.getProperty("java.io.tmpdir"),
    output      : String               = System.getProperty("java.io.tmpdir")
  ) extends FlinkConfig

  val c = Config()

  override val compiler = new RuntimeCompiler(codegenDir) with LabyrinthCompiler

  import compiler._
  import u.reify

  protected override def wrapInClass(tree: u.Tree): u.Tree = {
    import u.Quasiquote

    val Cls = api.TypeName(UUID.randomUUID().toString)
    val run = api.TermName(RuntimeCompiler.default.runMethod)
    val prs = api.Tree.closure(tree).map { sym =>
      val x = sym.name
      val T = sym.info
      q"val $x: $T"
    }

    q"""
    class $Cls {
      def $run(..$prs)(implicit flink: ${api.Type[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]}) =
        $tree
    }
    """
  }

  def withBackendContext[T](f: Env => T): T =
    withDefaultFlinkStreamEnv(f)


  val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.anf,
      Core.unnest
    ).compose(_.tree)

  val noopPipeline: u.Expr[Any] => u.Tree = pipeline(typeCheck = true)().compose(_.tree)

  def applyXfrm(xfrm: Xfrm): u.Expr[Any] => u.Tree = {

    pipeline(typeCheck = true)(
      Core.lnf,
      xfrm.timed,
      Core.unnest
    ).compose(_.tree)
  }

  def applyLabynization(): u.Expr[Any] => u.Tree = {
    pipeline(typeCheck = true)(
      Core.lnf,
      labyrinthNormalize.timed,
      labyrinthLabynize.timed
    ).compose(_.tree)
  }

  def applyLabynizationOnly(): u.Expr[Any] => u.Tree = {
    pipeline(typeCheck = true)(
      labyrinthLabynize.timed
    ).compose(_.tree)
  }

  // ---------------------------------------------------------------------------
  // Spec tests
  // ---------------------------------------------------------------------------

  // helper
  def add1(x: Int) : Int = x + 1
  def str(x: Int) : String = x.toString
  def add(x: Int, y: Int) : Int = x + y
  def add(u: Int, v: Int, w: Int, x: Int, y: Int, z: Int)(m: Int, n: Int)(s: Int, t: Int) : Int =
    u + v + w + x + y + z + m + n + s + t

  // actual tests
  "normalization" - {

    "ValDef only" in {
      val inp = reify {
        val a = 1
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = 1; tmp
        })
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "replace refs on valdef rhs" in {
      val inp = reify {
        val a = 1; val b = a; val c = a; b
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = 1; tmp
        });
        val b = a;
        val c = a;
        b
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "ValDef only, SingSrc rhs" in {
      val inp = reify {
        val a = 1;
        val b = DB.singSrc(() => {
          val tmp = 2; tmp
        })
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = 1; tmp
        });
        val b = DB.singSrc(() => {
          val tmp = 2; tmp
        })
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "ValDef only, DataBag rhs" in {
      val inp = reify {
        val a = 1
        val b = DataBag(Seq(2))
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = 1; tmp
        })
        val s = DB.singSrc(() => {
          val tmp = Seq(2); tmp
        })
        val sb = DB.fromSingSrcApply(s)
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "ValDef only, DataBag rhs 2" in {
      val inp = reify {
        val fun = add1(2)
        val s = Seq(fun)
        val b = DataBag(s)
      }
      val exp = reify {
        val lbdaFun = () => {
          val tmp = add1(2); tmp
        }
        val dbFun = DB.singSrc(lbdaFun)
        val dbs = dbFun.map(e => Seq(e))
        val res = DB.fromSingSrcApply(dbs)
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "replace refs simple" in {
      val inp = reify {
        val a = 1; a
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = 1; tmp
        }); a
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method one argument" in {
      val inp = reify {
        val a = 1;
        val b = add1(a);
        b
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = 1; tmp
        });
        val b = a.map(e => add1(e));
        b
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method one argument 2" in {
      val inp = reify {
        val a = new TestInt(1);
        val b = a.add1()
        a
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = new TestInt(1); tmp
        });
        val b = a.map((e: TestInt) => e.add1());
        a
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method fold1" in {
      val inp = reify {
        val s = Seq(1)
        val b = DataBag(s)
        val count = Count[Int](_ => true)
        val c: Long = b.fold(count)
        c
      }
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = Seq(1); tmp })
        val b: DataBag[Int] = DB.fromSingSrcApply(a)
        val count = Count[Int](_ => true)
        val c: DataBag[Long] = DB.fold1[Int, Long](b, count)
        c
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method fold2" in {
      val inp = reify {
        val s = Seq(1)
        val b = DataBag(s)
        val c: Int = b.fold(0)(i => i, (a, b) => a + b)
        c
      }
      val exp = reify {
        val s = DB.singSrc(() => { val tmp = Seq(1); tmp })
        val b: DataBag[Int] = DB.fromSingSrcApply(s)
        val c: DataBag[Int] = DB.fold2[Int, Int](b, 0, (i: Int) => i, (a, b) => a + b)
        c
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method collect" in {
      val inp = reify {
        val s = Seq(1)
        val b = DataBag(s)
        val c = b.collect()
        c
      }
      val exp = reify {
        val s = DB.singSrc(() => { val tmp = Seq(1); tmp })
        val b: DataBag[Int] = DB.fromSingSrcApply(s)
        val c = DB.collect(b)
        c
      }
      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method one argument typechange" in {
      val inp = reify {
        val a = 1;
        val b = str(a);
        b
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = 1; tmp
        });
        val b = a.map(e => str(e));
        b
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method two arguments no constant" in {
      val inp = reify {
        val a = 1
        val b = 2
        val c = add(a, b)
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = 1; tmp
        })
        val b = DB.singSrc(() => {
          val tmp = 2; tmp
        })
        val c = cross(a, b).map((t: (Int, Int)) => add(t._1, t._2))
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method two arguments with constants" in {
      val inp = reify {
        val a = 1
        val b = 2
        val c = add(3, 4, a, 5, 6, 7)(8, 9)(10, b)
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = 1; tmp
        })
        val b = DB.singSrc(() => {
          val tmp = 2; tmp
        })
        val c = cross(a, b).map((t: (Int, Int)) => add(3, 4, t._1, 5, 6, 7)(8, 9)(10, t._2))
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method two arguments 2" in {
      val inp = reify {
        val a = new TestInt(1)
        val b = 2
        val c = a.addd(1, b, 3)(4, 5)(6, 7)
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = new TestInt(1); tmp
        })
        val b = DB.singSrc(() => {
          val tmp = 2; tmp
        })
        val c = cross(a, b).map((t: (TestInt, Int)) => t._1.addd(1, t._2, 3)(4, 5)(6, 7))
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method three arguments with constants" in {
      val inp = reify {
        val a = 1
        val b = 2
        val c = 3
        val d = add(3, 4, a, 5, 6, 7)(c, 9)(10, b)
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = 1; tmp
        })
        val b = DB.singSrc(() => {
          val tmp = 2; tmp
        })
        val c = DB.singSrc(() => {
          val tmp = 3; tmp
        })
        val d = DB.cross3(a, c, b).map((t: (Int, Int, Int)) => add(3, 4, t._1, 5, 6, 7)(t._2, 9)(10, t._3))
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method three arguments 2" in {
      val inp = reify {
        val a = new TestInt(1)
        val b = 2
        val c = 3
        val d = a.addd(1, b, 3)(4, c)(6, 7)
      }
      val exp = reify {
        val a = DB.singSrc(() => {
          val tmp = new TestInt(1); tmp
        })
        val b = DB.singSrc(() => {
          val tmp = 2; tmp
        })
        val c = DB.singSrc(() => {
          val tmp = 3; tmp
        })
        val d = DB.cross3(a, b, c).map((t: (TestInt, Int, Int)) => t._1.addd(1, t._2, 3)(4, t._3)(6, 7))
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }
  }

  implicit val defaultEnv = defaultFlinkStreamEnv

  "labynization" - {

    /*
    for now
    .setparallelism(1)
    partitioner always0 with para = 1
     */

    "lambda only" in {
      val inp = reify {
        val a = () => 1
      }

      val exp = reify {
        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(0)
        LabyStatics.setKickoffSource(0)
        val a = () => 1
        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        val exec = LabyStatics.executeWithCatch(env)
        exec
      }

      applyLabynization()(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "singSrc rhs" in {
      val inp = reify {
        val a = 1
      }

      val exp = reify {

        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(0)
        LabyStatics.setKickoffSource(0)
        val a = new LabyNode[labyrinth.util.Nothing, Int](
          "fromNothing",
          ScalaOps.fromNothing[Int]( () => { val tmp = 1; tmp } ),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .setParallelism(1)


        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        LabyStatics.executeWithCatch(env)
      }

      applyLabynization()(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "databag rhs" in {
      val inp = reify {
        val s = Seq(1)
        val db = DataBag(s)
      }

      val exp = reify {

        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(0)
        LabyStatics.setKickoffSource(0)
        val n1 = new LabyNode[labyrinth.util.Nothing, Seq[Int]](
          "fromNothing",
          ScalaOps.fromNothing[Seq[Int]](() => {
            val tmp = Seq(1); tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Seq[Int]](Memo.typeInfoForType[Seq[Int]])
        )
          .setParallelism(1)

        val n2 = new LabyNode[Seq[Int], Int](
          "fromSingSrcApply",
          ScalaOps.fromSingSrcApply[Int, Seq[Int]](),
          0,
          new Always0[Seq[Int]](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .addInput(n1, true, false)
          .setParallelism(1)


        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        LabyStatics.executeWithCatch(env)
      }

      applyLabynization()(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "map" in {
      val inp = reify {
        val s = Seq(1,2,3)
        val db = DataBag(s)
        val dbm = db.map(x => add1(x))
      }

      val exp = reify {

        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(0)
        LabyStatics.setKickoffSource(0)

        val n1 = new LabyNode[labyrinth.util.Nothing, Seq[Int]](
          "fromNothing",
          ScalaOps.fromNothing[Seq[Int]](() => {
            val tmp = Seq(1,2,3); tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Seq[Int]](Memo.typeInfoForType[Seq[Int]])
        )
          .setParallelism(1)

        val n2 = new LabyNode[Seq[Int], Int](
          "fromSingSrcApply",
          ScalaOps.fromSingSrcApply[Int, Seq[Int]](),
          0,
          new Always0[Seq[Int]](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .addInput(n1, true, false)
          .setParallelism(1)

        val n3 = new LabyNode[Int, Int](
          "map",
          ScalaOps.map(x => add1(x)),
          0,
          new Always0[Int](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .addInput(n2, true, false)
          .setParallelism(1)

        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        LabyStatics.executeWithCatch(env)
      }

      applyLabynization()(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "flatmap" in {
      val inp = reify {
        val s = Seq(1, 2, 3)
        val db = DataBag(s)
        val dbm = db.flatMap(x => DataBag(Seq(0, add1(x))))
      }

      val exp = reify {

        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(0)
        LabyStatics.setKickoffSource(0)

        val n1 = new LabyNode[labyrinth.util.Nothing, Seq[Int]](
          "fromNothing",
          ScalaOps.fromNothing[Seq[Int]](() => {
            val tmp = Seq(1, 2, 3);
            tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Seq[Int]](Memo.typeInfoForType[Seq[Int]])
        )
          .setParallelism(1)

        val n2 = new LabyNode[Seq[Int], Int](
          "fromSingSrcApply",
          ScalaOps.fromSingSrcApply[Int, Seq[Int]](),
          0,
          new Always0[Seq[Int]](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .addInput(n1, true, false)
          .setParallelism(1)

        val n3 = new LabyNode[Int, Int](
          "flatMap",
          ScalaOps.flatMapDataBagHelper(x => DataBag(Seq(0, add1(x)))),
          0,
          new Always0[Int](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .addInput(n2, true, false)
          .setParallelism(1)

        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        LabyStatics.executeWithCatch(env)
      }

      applyLabynization()(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "cross" in {
      val inp = reify {
        val db1 = DataBag(Seq(1,2,3))
        val db2 = DataBag(Seq("1","2","3"))
        val dbc = cross(db1, db2)
      }

      val exp = reify {

        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(0)
        LabyStatics.setKickoffSource(0)


        val n1_1 = new LabyNode[labyrinth.util.Nothing, Seq[Int]](
          "fromNothing",
          ScalaOps.fromNothing[Seq[Int]](() => {
            val tmp = Seq(1,2,3); tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Seq[Int]](Memo.typeInfoForType[Seq[Int]])
        )
          .setParallelism(1)

        val n1_2 = new LabyNode[Seq[Int], Int](
          "fromSingSrcApply",
          ScalaOps.fromSingSrcApply[Int, Seq[Int]](),
          0,
          new Always0[Seq[Int]](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .addInput(n1_1, true, false)
          .setParallelism(1)

        val n2_1 = new LabyNode[labyrinth.util.Nothing, Seq[String]](
          "fromNothing",
          ScalaOps.fromNothing[Seq[String]](() => {
            val tmp = Seq("1","2","3"); tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Seq[String]](Memo.typeInfoForType[Seq[String]])
        )
          .setParallelism(1)

        val n2_2 = new LabyNode[Seq[String], String](
          "fromSingSrcApply",
          ScalaOps.fromSingSrcApply[String, Seq[String]](),
          0,
          new Always0[Seq[String]](1),
          null,
          new ElementOrEventTypeInfo[String](Memo.typeInfoForType[String])
        )
          .addInput(n2_1, true, false)
          .setParallelism(1)

        val n1_3 = new LabyNode[Int, Either[Int,String]](
          "map",
          ScalaOps.map(i => scala.util.Left(i)),
          0,
          new Always0[Int](1),
          null,
          new ElementOrEventTypeInfo[Either[Int,String]](Memo.typeInfoForType[Either[Int,String]])
        )
          .addInput(n1_2, true, false)
          .setParallelism(1)

        val n2_3 = new LabyNode[String, Either[Int,String]](
          "map",
          ScalaOps.map(s => scala.util.Right(s)),
          0,
          new Always0[String](1),
          null,
          new ElementOrEventTypeInfo[Either[Int,String]](Memo.typeInfoForType[Either[Int,String]])
        )
          .addInput(n2_2, true, false)
          .setParallelism(1)

        val n_cross = new LabyNode[Either[Int, String], (Int, String)](
          "cross",
          ScalaOps.cross[Int, String],
          0,
          new Always0[Either[Int,String]](1),
          null,
          new ElementOrEventTypeInfo[(Int, String)](Memo.typeInfoForType[(Int, String)])
        )
          .addInput(n1_3, true, false)
          .addInput(n2_3, true, false)
          .setParallelism(1)

        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        LabyStatics.executeWithCatch(env)

      }
      applyLabynization()(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "equiJoin" in {
      val inp = reify {
        val db1 = DataBag(Seq("a", "bb", "ddd"))
        val db2 = DataBag(Seq(2 , 5, 2))
        val extrA = (t: String) => t.length
        val extrB = (t: Int) => t
        val dbc = equiJoin[String, Int, Int](extrA, extrB )(db1, db2)
      }

      val exp = reify {

        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(0)
        LabyStatics.setKickoffSource(0)


        val n1_1 = new LabyNode[labyrinth.util.Nothing, Seq[String]](
          "fromNothing",
          ScalaOps.fromNothing[Seq[String]](() => {
            val tmp = Seq("a", "bb", "ddd"); tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Seq[String]](Memo.typeInfoForType[Seq[String]])
        )
          .setParallelism(1)


        val n1_2 = new LabyNode[Seq[String], String](
          "fromSingSrcApply",
          ScalaOps.fromSingSrcApply[String, Seq[String]](),
          0,
          new Always0[Seq[String]](1),
          null,
          new ElementOrEventTypeInfo[String](Memo.typeInfoForType[String])
        )
          .addInput(n1_1, true, false)
          .setParallelism(1)


        val n2_1 = new LabyNode[labyrinth.util.Nothing, Seq[Int]](
          "fromNothing",
          ScalaOps.fromNothing[Seq[Int]](() => {
            val tmp = Seq(2 , 5, 2); tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Seq[Int]](Memo.typeInfoForType[Seq[Int]])
        )
          .setParallelism(1)

        val n2_2 = new LabyNode[Seq[Int], Int](
          "fromSingSrcApply",
          ScalaOps.fromSingSrcApply[Int, Seq[Int]](),
          0,
          new Always0[Seq[Int]](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .addInput(n2_1, true, false)
          .setParallelism(1)

        val extrA = (t: String) => t.length
        val extrB = (t: Int) => t

        val n1_3 = new LabyNode[String, Either[String, Int]](
          "map",
          ScalaOps.map(i => scala.util.Left(i)),
          0,
          new Always0[String](1),
          null,
          new ElementOrEventTypeInfo[Either[String, Int]](Memo.typeInfoForType[Either[String, Int]])
        )
          .addInput(n1_2, true, false)
          .setParallelism(1)

        val n2_3 = new LabyNode[Int, Either[String, Int]](
          "map",
          ScalaOps.map(s => scala.util.Right(s)),
          0,
          new Always0[Int](1),
          null,
          new ElementOrEventTypeInfo[Either[String, Int]](Memo.typeInfoForType[Either[String, Int]])
        )
          .addInput(n2_2, true, false)
          .setParallelism(1)

        val n_join = new LabyNode[Either[String, Int], (String,Int)](
          "join",
          ScalaOps.joinScala[String, Int, Int](extrA, extrB),
          0,
          new Always0[Either[String, Int]](1),
          null,
          new ElementOrEventTypeInfo[(String, Int)](Memo.typeInfoForType[(String, Int)])
        )
          .addInput(n1_3, true, false)
          .addInput(n2_3, true, false)
          .setParallelism(1)

        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        LabyStatics.executeWithCatch(env)

      }
      applyLabynization()(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "fold1" in {
      val inp = reify {
        val s = Seq(1)
        val b = DataBag(s)
        val count = Count[Int](_ => true)
        val c: Long = b.fold(count)
      }

      val exp = reify {

        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(0)
        LabyStatics.setKickoffSource(0)

        val n1 = new LabyNode[labyrinth.util.Nothing, Seq[Int]](
          "fromNothing",
          ScalaOps.fromNothing[Seq[Int]](() => {
            val tmp = Seq(1); tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Seq[Int]](Memo.typeInfoForType[Seq[Int]])
        )
          .setParallelism(1)

        val n2 = new LabyNode[Seq[Int], Int](
          "fromSingSrcApply",
          ScalaOps.fromSingSrcApply[Int, Seq[Int]](),
          0,
          new Always0[Seq[Int]](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .addInput(n1, true, false)
          .setParallelism(1)

        val count = Count[Int](_ => true)

        val c = new LabyNode[Int, Long](
          "fold1",
          ScalaOps.foldAlgHelper(count),
          0,
          new Always0[Int](1),
          null,
          new ElementOrEventTypeInfo[Long](Memo.typeInfoForType[Long])
        )
          .addInput(n2, true, false)
          .setParallelism(1)

        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        LabyStatics.executeWithCatch(env)
      }

      applyLabynization()(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "fold2" in {
      val inp = reify {
        val s = Seq("foo")
        val b = DataBag(s)
        val c: Int = b.fold(0)((s: String) => s.length, (a, b) => a + b)
      }

      val exp = reify {

        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(0)
        LabyStatics.setKickoffSource(0)

        val n1 = new LabyNode[labyrinth.util.Nothing, Seq[String]](
          "fromNothing",
          ScalaOps.fromNothing[Seq[String]](() => {
            val tmp = Seq("foo"); tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Seq[String]](Memo.typeInfoForType[Seq[String]])
        )
          .setParallelism(1)

        val n2 = new LabyNode[Seq[String], String](
          "fromSingSrcApply",
          ScalaOps.fromSingSrcApply[String, Seq[String]](),
          0,
          new Always0[Seq[String]](1),
          null,
          new ElementOrEventTypeInfo[String](Memo.typeInfoForType[String])
        )
          .addInput(n1, true, false)
          .setParallelism(1)

        val c = new LabyNode[String, Int](
          "fold2",
          ScalaOps.fold(0, (s: String) => s.length, (a,b) => a + b),
          0,
          new Always0[String](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .addInput(n2, true, false)
          .setParallelism(1)

        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        LabyStatics.executeWithCatch(env)
      }

      applyLabynization()(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "read Text" in {
      val inp = reify {
        val p = "path"
        val rt = DataBag.readText(p)
      }

      val exp = reify {

        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(0)
        LabyStatics.setKickoffSource(0)

        val n1 = new LabyNode[labyrinth.util.Nothing, String](
          "fromNothing",
          ScalaOps.fromNothing[String](() => {
            val tmp = "path"; tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[String](Memo.typeInfoForType[String])
        )
          .setParallelism(1)

        val n2 = new LabyNode[String, InputFormatWithInputSplit[String, FileInputSplit]](
          "inputSplits",
          ScalaOps.textSource,
          0,
          new Always0[String](1),
          null,
          new ElementOrEventTypeInfo[InputFormatWithInputSplit[String, FileInputSplit]](
            Memo.typeInfoForType[InputFormatWithInputSplit[String, FileInputSplit]]
          )
        )
          .addInput(n1, true, false)
          .setParallelism(1)

        val n3 = new LabyNode[InputFormatWithInputSplit[String, FileInputSplit], String](
          "readSplits",
          ScalaOps.textReader,
          0,
          new Always0[InputFormatWithInputSplit[String, FileInputSplit]](1),
          null,
          new ElementOrEventTypeInfo[String](Memo.typeInfoForType[String])
        )
          .addInput(n2, true, false)
          .setParallelism(1)

        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        LabyStatics.executeWithCatch(env)
      }

      applyLabynization()(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "write csv" in {
      val inp = reify {
        val fun1: () => Seq[Int] = () => { val tmp = Seq(1, 2); tmp }
        val d = DB.singSrc[Seq[Int]](fun1)
        val db = DB.fromSingSrcApply[Int](d)
        val fun2: () => String = () => { val tmp = "path"; tmp }
        val p = DB.singSrc[String](fun2)
        val fun3: () => CSV = () => {val tmp = csvDummy; tmp }
        val csv = DB.singSrc[org.emmalanguage.io.csv.CSV](fun3)
        val s = DB.fromDatabagWriteCSV(db, p, csv)
      }

      val exp = reify {

        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(0)
        LabyStatics.setKickoffSource(0)

        val nData = new LabyNode[labyrinth.util.Nothing, Seq[Int]](
          "fromNothing",
          ScalaOps.fromNothing[Seq[Int]](() => {
            val tmp = Seq(1, 2); tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Seq[Int]](Memo.typeInfoForType[Seq[Int]])
        )
          .setParallelism(1)

        val nDataFSS = new LabyNode[Seq[Int], Int](
          "fromSingSrcApply",
          ScalaOps.fromSingSrcApply[Int, Seq[Int]](),
          0,
          new Always0[Seq[Int]](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .addInput(nData, true, false)
          .setParallelism(1)

        val nPath = new LabyNode[labyrinth.util.Nothing, String](
          "fromNothing",
          ScalaOps.fromNothing[String](() => {
            val tmp = "path"; tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[String](Memo.typeInfoForType[String])
        )
          .setParallelism(1)

        val nCSV = new LabyNode[labyrinth.util.Nothing, org.emmalanguage.io.csv.CSV](
          "fromNothing",
          ScalaOps.fromNothing[org.emmalanguage.io.csv.CSV](() => {
            val tmp = csvDummy; tmp
          }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[org.emmalanguage.io.csv.CSV](Memo.typeInfoForType[org.emmalanguage.io.csv.CSV])
        )
          .setParallelism(1)

        val dataEither = new LabyNode[Int, Either[Int,org.emmalanguage.io.csv.CSV]](
          "map",
          ScalaOps.map(i => scala.util.Left(i)),
          0,
          new Always0[Int](1),
          null,
          new ElementOrEventTypeInfo[Either[Int,org.emmalanguage.io.csv.CSV]](
            Memo.typeInfoForType[Either[Int,org.emmalanguage.io.csv.CSV]]
          )
        )
          .addInput(nDataFSS, true, false)
          .setParallelism(1)

        val csvEither = new LabyNode[org.emmalanguage.io.csv.CSV, Either[Int,org.emmalanguage.io.csv.CSV]](
          "map",
          ScalaOps.map(s => scala.util.Right(s)),
          0,
          new Always0[org.emmalanguage.io.csv.CSV](1),
          null,
          new ElementOrEventTypeInfo[Either[Int,org.emmalanguage.io.csv.CSV]](
            Memo.typeInfoForType[Either[Int,org.emmalanguage.io.csv.CSV]]
          )
        )
          .addInput(nCSV, true, false)
          .setParallelism(1)

        val nToCsvString = new LabyNode[Either[Int, CSV], String](
          "toCsvString",
          ScalaOps.toCsvString[Int],
          0,
          new Always0[Either[Int, CSV]](1),
          null,
          new ElementOrEventTypeInfo[String](Memo.typeInfoForType[String])
        )
          .addInput(dataEither, true, false)
          .addInput(csvEither, true, false)
          .setParallelism(1)

        val nStringSink = new LabyNode[String, Unit](
          "stringFileSink",
          ScalaOps.writeString,
          0,
          new Always0[String](1),
          null,
          new ElementOrEventTypeInfo[Unit](Memo.typeInfoForType[Unit])
        )
          .addInput(nPath, true, false)
          .addInput(nToCsvString, true, false)
          .setParallelism(1)

        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        LabyStatics.executeWithCatch(env)

      }

      applyLabynizationOnly()(inp) shouldBe alphaEqTo(anfPipeline(exp))

    }

  }

  "control flow" - {

    "with trivial body" in {
      val inp = reify {
        //        var i = 0
        //        while (i < 100) i += 1
        //        println(i)

        @whileLoop def while$1(i: Int): Unit = {
          val x$1 = i < 100
          @loopBody def body$1(): Unit = {
            val i$3 = i + 1
            while$1(i$3)
          }
          @suffix def suffix$1(): Unit = {
            println(i)
          }
          if (x$1) body$1()
          else suffix$1()
        }
        while$1(0)
      }

      val exp = reify {

        // outer    -> 0
        // while$1  -> 1
        // body$1   -> 2
        // suffix$1 -> 3

        // kickoff:  0
        // terminal: 3


        LabyStatics.registerCustomSerializer()
        LabyStatics.setTerminalBbid(3)
        LabyStatics.setKickoffSource(0, 1)

        val n1 = new LabyNode[labyrinth.util.Nothing, Int](
          "fromNothing",
          ScalaOps.fromNothing(() => {val tmp = 0; tmp }),
          0,
          new Always0[labyrinth.util.Nothing](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .setParallelism(1)

        val iPhi = LabyStatics.phi[Int](
          "arg$r1Phi",
          1,
          new Always0[Int](1),
          null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )

        val x$1 = new LabyNode[Int, Boolean](
          "map",
          ScalaOps.map(_ < 100),
          1,
          new Always0[Int](1),
          null,
          new ElementOrEventTypeInfo[Boolean](Memo.typeInfoForType[Boolean])
        )
          .addInput(iPhi, true, false)
          .setParallelism(1)

        val i$3 = new LabyNode[Int, Int](
          "map",
          ScalaOps.map(_ + 1),
          2,
          new Always0[Int](1), null,
          new ElementOrEventTypeInfo[Int](Memo.typeInfoForType[Int])
        )
          .addInput(iPhi, false, true)
          .setParallelism(1)

        val addInp1 = iPhi.addInput(i$3, false, true)

        val printlnNode = new LabyNode[Int, Unit](
          "map",
          ScalaOps.map( (i:Int) => println(i)),
          3,
          new Always0[Int](1),
          null,
          new ElementOrEventTypeInfo[Unit](Memo.typeInfoForType[Unit])
        )
          .addInput(iPhi, false, true)
          .setParallelism(1)

        val ifCondNode = new LabyNode(
          "condNode",
          ScalaOps.condNode(
            Seq(2, 1),
            Seq(3)
          ),
          1,
          new Always0[Boolean](1),
          null,
          new ElementOrEventTypeInfo[labyrinth.util.Unit](Memo.typeInfoForType[labyrinth.util.Unit])
        )
          .addInput(x$1, true, false)
          .setParallelism(1)

        val addInp2 = iPhi.addInput(n1, false, true)

        val env = implicitly[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]
        LabyStatics.translateAll
        LabyStatics.executeWithCatch(env)

      }

      applyLabynization()(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }
    
  }

  def expandAndAnf(t: u.Tree) : u.Tree = {
    val tt = compiler.unTypeCheck(t)
    pipeline(typeCheck = true)(
      Core.anf,
      Core.unnest
    )(tt)
  }

  val csvDummy = CSV()
}

case class Edge[V](src: V, dst: V)
case class LEdge[V, L](@emma.pk src: V, @emma.pk dst: V, label: L)
case class LVertex[V, L](@emma.pk id: V, label: L)
case class Triangle[V](x: V, y: V, z: V)
case class Message[K, V](@emma.pk tgt: K, payload: V)