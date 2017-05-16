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
package api

import cogadb.CoGaDB
import compiler.lang.cogadb.ast

import org.scalatest._

class RuntimeSpec extends FreeSpec with Matchers with CoGaDBSpec {


  "create and fetch tuples of strings" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq(("foo", "bar"), ("Hello", "World"))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch triples of strings" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq(("foo", "bar"), ("Hello", "World"), ("These are", "Two words"))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }

  "create and fetch tuples of doubles" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1.1, 2.1), (2.1, 3.1))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch triples of doubles" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1.1, 2.1, 3.1), (1.2, 2.2, 3.2))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }

  "create and fetch pair of ints and strings" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1, "Foo"), (2, "Bar"))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch pair of ints and char" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1, 'F'), (2, 'B'))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch tuples of ints and floats" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1, 1.1F), (2, 2.2F))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch tuples of floats" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1.1F, 1.2F), (2.1F, 2.2F))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch triples of floats" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1.1F, 1.2F, 1.3F), (2.1F, 2.2F, 2.3F))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch quadruples of floats" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1.1F, 1.2F, 1.3F, 1.4F), (2.1F, 2.2F, 2.3F, 2.4F))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch quintuples of floats" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1.1F, 1.2F, 1.3F, 1.4F, 1.5F), (2.1F, 2.2F, 2.3F, 2.4F, 2.5F))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch sextuples of floats" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1.1F, 1.2F, 1.3F, 1.4F, 1.5F, 1.6F), (2.1F, 2.2F, 2.3F, 2.4F, 2.5F, 2.6F))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }

  "create and fetch tuples of ints" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1, 2), (3, 4))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch triples of ints" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1, 2, 3), (7, 8, 9))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch quintuples of ints" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1, 2, 3, 4, 5), (7, 8, 9, 10, 11))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }
  "create and fetch sextuples of ints" in withCoGaDB { implicit cogadb: CoGaDB =>
    val exp = Seq((1, 2, 3, 4, 5, 6), (7, 8, 9, 10, 11, 12))
    val act = CoGaDBTable(exp).fetch()

    act shouldBe exp
  }


  "test cross join" in withCoGaDB { implicit cogadb: CoGaDB =>
    val aSeq = Seq((1, 10), (2, 20))
    val bSeq = Seq((3, 30), (4, 40))

    val as = new CoGaDBTable[(Int, String)](cogadb.importSeq(aSeq))
    val bs = new CoGaDBTable[(Int, String)](cogadb.importSeq(bSeq))

    val act = new CoGaDBTable[(Int, Int, Int, Int)]({
      ast.CrossJoin(as.rep, bs.rep)
    })
    //val res = cogadb.exportSeq[(Int, Int, Int, Int)](ast.CrossJoin(as.rep, bs.rep))

    val exp = for (a <- aSeq; b <- bSeq) yield (a._1, a._2, b._1, b._2)

    act.fetch().toList should contain theSameElementsAs exp
  }

  "create and fetch empty" in withCoGaDB { implicit cogadb: CoGaDB =>
    CoGaDBTable.empty[Int].fetch() shouldBe Seq.empty[Int]
  }

  "test join" in withCoGaDB { implicit cogadb: CoGaDB =>

    val as = new CoGaDBTable[(Int, String)](cogadb.importSeq(Seq((1, "Foo"), (2, "Hello"))))
    val bs = new CoGaDBTable[(Int, String)](cogadb.importSeq(Seq((1, "Bar"), (2, "World"))))

    val act = new CoGaDBTable[(Int, String, String)]({
      val joinPred = Seq(
        ast.ColCol(
          lhs = as.ref("_1"),
          rhs = bs.ref("_1"),
          cmp = ast.Equal
        )
      )
      val projectedFields = Seq(
        as.ref("_1", "_1"),
        as.ref("_2", "_2"),
        bs.ref("_2", "_3")
      )

      ast.Projection(projectedFields,
        ast.Join("INNER_JOIN", joinPred, as.rep, bs.rep))
    })

    val exp = for {
      a <- as.fetch()
      b <- bs.fetch()
      if a._1 == b._1
    } yield (a._1, a._2, b._2)

    act.fetch() should contain theSameElementsAs exp

  }

  "filter test" in withCoGaDB { implicit cogadb: CoGaDB =>
    val as = new CoGaDBTable[(Int, String)](cogadb.importSeq(Seq((1, "Foo"), (2, "Hello"), (3, "Baz"))))
    val bs = new CoGaDBTable[(Int, String)](cogadb.importSeq(Seq((1, "Bar"), (2, "World"), (3, "Quux"))))
    val cs = new CoGaDBTable[(Int, String)](cogadb.importSeq(Seq((2, "true"), (3, "false"))))


    val act = new CoGaDBTable[(Int, String, String)]({

      ast.Selection(
        Seq(
          ast.ColConst(
            attr = cs.ref("_2"),
            const = ast.VarCharConst("true"),
            cmp = ast.Equal
          )),
        ast.Projection(
          Seq(
            as.ref("_1", "_1"),
            as.ref("_2", "_2"),
            bs.ref("_2", "_3")
          ),
          ast.Join("INNER_JOIN",
            Seq(
              ast.ColCol(
                lhs = cs.ref("_1"),
                rhs = as.ref("_1"),
                cmp = ast.Equal
              )),
            cs.rep,
            ast.Projection(
              Seq(
                as.ref("_1", "_1"),
                as.ref("_2", "_2"),
                bs.ref("_2", "_3")
              ),
              ast.Join("INNER_JOIN",
                Seq(
                  ast.ColCol(
                    lhs = as.ref("_1"),
                    rhs = bs.ref("_1"),
                    cmp = ast.Equal
                  )),
                ast.Selection(
                  Seq(
                    ast.ColConst(
                      attr = as.ref("_1"),
                      const = ast.IntConst(1),
                      cmp = ast.GreaterThan
                    )),
                  as.rep), bs.rep)))))
    })

    val exp = for {
      a <- as.fetch().filter(x => x._1 > 1)
      b <- bs.fetch()
      c <- cs.fetch()
      if a._1 == b._1 && a._1 == c._1 && c._2 == "true"
    } yield (a._1, a._2, b._2)

    act.fetch() should contain theSameElementsAs (exp)
  }

  "test 3-way join" ignore withCoGaDB { implicit cogadb: CoGaDB =>

    val as = new CoGaDBTable[(Int, String)](cogadb.importSeq(Seq((1, "Foo"), (2, "Hello"))))
    val bs = new CoGaDBTable[(Int, String)](cogadb.importSeq(Seq((1, "Bar"), (2, "World"))))
    val cs = new CoGaDBTable[(Int, String, Int)](cogadb.importSeq(Seq((1, "Baz", 1), (2, "!!!!", 2))))

    val joinedAsBs = new CoGaDBTable[(Int, String, String)]({
      val joinPredAsBs = Seq(
        ast.ColCol(
          lhs = as.ref("_1"),
          rhs = bs.ref("_1"),
          cmp = ast.Equal
        )
      )

      val projectedFieldsAsBs = Seq(
        as.ref("_1", "_1"),
        as.ref("_2", "_2"),
        bs.ref("_2", "_3")
      )

      ast.Projection(projectedFieldsAsBs,
        ast.Join("INNER_JOIN", joinPredAsBs, as.rep, bs.rep))
    })

    val act = new CoGaDBTable[(Int, String, String, String)]({
      val joinPredBsCs = Seq(
        ast.ColCol(
          lhs = joinedAsBs.ref("_1"),
          rhs = cs.ref("_3"),
          cmp = ast.Equal
        ))

      val projectedFieldsAsBsCs = Seq(
        joinedAsBs.ref("_1", "_1"),
        joinedAsBs.ref("_2", "_2"),
        joinedAsBs.ref("_3", "_3"),
        cs.ref("_2", "_4")
      )

      ast.Projection(projectedFieldsAsBsCs,
        ast.Join("INNER_JOIN", joinPredBsCs, joinedAsBs.rep, cs.rep)
      )
    })

    val exp = for {
      a <- as.fetch()
      b <- bs.fetch()
      c <- cs.fetch()
      if a._1 == b._1
      if c._1 == a._1
    } yield (a._1, a._2, b._2, c._2)

    act.fetch() should contain theSameElementsAs exp

  }

}