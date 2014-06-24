package eu.stratosphere.emma

import _root_.eu.stratosphere.emma.ir._

import _root_.scala.collection.mutable.ListBuffer
import _root_.scala.reflect.runtime.universe._

object App {

  case class Person(age: Int, children: Int, name: String, surname: String)

  def main(args: Array[String]) {

    // reference dataflow #01
    {
      val df = reference01()
      println("// reference dataflow #01")
      prettyprint(df)
      println("")
    }

    // reference dataflow #02
    {
      val df = reference02()
      println("// reference dataflow #02")
      prettyprint(df)
      println("")
    }

    // dataflow #01
    //   src(A) — map — filter — filter — sink(C)
    {
      val df = dataflow {
        val A = read("file:///tmp/emma/people.csv", new InputFormat[Person])
        val B = for (a <- A; if a.children < 1; if a.age > 30) yield s"${a.name} ${a.surname}"
        val C = write("file:///tmp/emma/result.csv", new OutputFormat[String])(B)

        C
      }

      println("// dataflow #01")
      prettyprint(df)
      println("")
    }

    // dataflow #02
    //   src(A) — cross — filter — B
    //            /
    //   src(A) —’
    {
      val df = dataflow {
        val A = read("file:///tmp/emma/people.csv", new InputFormat[Person])
        val B = cross(A, A).withFilter(x => x._1.age == x._2.age)
        val C = write("file:///tmp/emma/result.csv", new OutputFormat[(Person, Person)])(B)

        C
      }
      println("// dataflow #02")
      prettyprint(df)
      println("")
    }

    // dataflow #03
    //   src(A) — cross — map — B
    //            /
    //   src(A) —’
    {
      val df = dataflow {
        val A = read("file:///tmp/emma/people.csv", new InputFormat[Person])
        val B = cross(A, A).map(x => Math.max(x._1.children, x._2.children))
        val C = write("file:///tmp/emma/result.csv", new OutputFormat[Int])(B)

        C
      }
      println("// dataflow #03")
      prettyprint(df)
      println("")
    }

    // dataflow #04
    //   src(A) — join — map — B
    //            /
    //   src(A) —’
    {
      val df = dataflow {
        val A = read("file:///tmp/emma/people.csv", new InputFormat[Person])
        val B = join[Person, Person, Int](a => a.age, a => a.age)(A, A).map(x => Math.max(x._1.children, x._1.children))
        val C = write("file:///tmp/emma/result.csv", new OutputFormat[Int])(B)

        C
      }
      println("// dataflow #04")
      prettyprint(df)
      println("")
    }

    // dataflow #05
    //   src(A) — distinct — distinct — sink(C)
    {
      val df = dataflow {
        val A = read("file:///tmp/emma/people.csv", new InputFormat[Person])
        val B = distinct {
          A.map(x => x.surname).distinct()
        }
        val C = write("file:///tmp/emma/result.csv", new OutputFormat[String])(B)

        C
      }

      println("// dataflow #05")
      prettyprint(df)
      println("")
    }

    // dataflow #06
    //   src(A) — groupBy — sink(C)
    {
      val df = dataflow {
        val A = read("file:///tmp/emma/people.csv", new InputFormat[Person])
        val B = A.groupBy(x => x.age)
        val C = write("file:///tmp/emma/result.csv", new OutputFormat[DataBag[Person]])(B)

        C
      }

      println("// dataflow #06")
      prettyprint(df)
      println("")
    }
  }

  /**
   * Hardwired construction of a reference dataflow.
   *
   * @return
   */
  private def reference01() = {

    val sinks = ListBuffer[Comprehension]()

    val _MC_00000_C = {
      val _MC_00001_B = {
        val x = null.asInstanceOf[Int]
        val y = null.asInstanceOf[Int]

        val _MC_00002_A = {
          val bind_bytes = ScalaExprGenerator("bytes", ScalaExpr("ifmt" :: "dop" :: Nil, {
            val ifmt = new InputFormat[Int]()
            val dop = null.asInstanceOf[Int]

            reify {
              ifmt.split("file:///tmp/emma/people.csv", dop)
            }
          }))

          val bind_record = ScalaExprGenerator("record", ScalaExpr("ifmt" :: "bytes" :: Nil, {
            val ifmt = new InputFormat[Int]()
            val bytes = null.asInstanceOf[Seq[Byte]]

            reify {
              ifmt.read(bytes)
            }
          }))

          val head = ScalaExpr("record" :: Nil, {
            val record = null.asInstanceOf[Int]

            reify {
              record
            }
          })

          new Comprehension(monad.Bag[Int], head, bind_bytes :: bind_record :: Nil)
        }

        val _MC_00003_A = {
          val bind_bytes = ScalaExprGenerator("bytes", ScalaExpr("ifmt" :: "dop" :: Nil, {
            val ifmt = new InputFormat[Int]()
            val dop = null.asInstanceOf[Int]

            reify {
              ifmt.split("file:///tmp/emma/people.csv", dop)
            }
          }))

          val bind_record = ScalaExprGenerator("record", ScalaExpr("ifmt" :: "bytes" :: Nil, {
            val ifmt = new InputFormat[Int]()
            val bytes = null.asInstanceOf[Seq[Byte]]

            reify {
              ifmt.read(bytes)
            }
          }))

          val head = ScalaExpr("record" :: Nil, {
            val record = null.asInstanceOf[Int]

            reify {
              record
            }
          })

          Comprehension(monad.Bag[Int], head, bind_bytes :: bind_record :: Nil)
        }

        val qualifiers = ListBuffer[Qualifier]()
        qualifiers += ComprehensionGenerator("x", _MC_00002_A)
        qualifiers += ComprehensionGenerator("y", _MC_00003_A)

        val head = ScalaExpr("y" :: "x" :: Nil, reify {
          (x, y)
        })

        Comprehension(monad.Bag[(Int, Int)], head, qualifiers.toList)
      }

      val bind_record = ComprehensionGenerator("record", _MC_00001_B)
      val head = ScalaExpr("ofmt" :: "record" :: Nil, {
        val ofmt = new OutputFormat[(Int, Int)]()
        val record = null.asInstanceOf[(Int, Int)]

        reify {
          ofmt.write(record)
        }
      })

      Comprehension(monad.All, head, bind_record :: Nil)
    }

    sinks += _MC_00000_C

    Dataflow("Emma Dataflow", sinks.toList)
  }

  /**
   * Hardwired construction of a reference dataflow.
   *
   * @return
   */
  private def reference02(): Dataflow = {

    val sinks = ListBuffer[Comprehension]()

    val _MC_00000_C = {
      val _MC_00001_B = {
        val x = null.asInstanceOf[Int]

        val _MC_00002_A = {
          val bind_bytes = ScalaExprGenerator("bytes", ScalaExpr("ifmt" :: "dop" :: Nil, {
            val ifmt = new InputFormat[Int]()
            val dop = null.asInstanceOf[Int]

            reify {
              ifmt.split("file:///tmp/emma/people.csv", dop)
            }
          }))

          val bind_record = ScalaExprGenerator("record", ScalaExpr("ifmt" :: "bytes" :: Nil, {
            val ifmt = new InputFormat[Int]()
            val bytes = null.asInstanceOf[Seq[Byte]]

            reify {
              ifmt.read(bytes)
            }
          }))

          val head = ScalaExpr("record" :: Nil, {
            val record = null.asInstanceOf[Int]

            reify {
              record
            }
          })

          Comprehension(monad.Bag[Int], head, bind_bytes :: bind_record :: Nil)
        }

        val qualifiers = ListBuffer[Qualifier]()
        qualifiers += ComprehensionGenerator("x", _MC_00002_A)

        val head = ScalaExpr(Nil, reify {
          x
        })

        Comprehension(monad.Bag[Int], head, qualifiers.toList)
      }

      val bind_record = ComprehensionGenerator("record", _MC_00001_B)
      val head = ScalaExpr("ofmt" :: "record" :: Nil, {
        val ofmt = new OutputFormat[Int]()
        val record = null.asInstanceOf[Int]

        reify {
          ofmt.write(record)
        }
      })

      Comprehension(monad.All, head, bind_record :: Nil)
    }

    sinks += _MC_00000_C

    Dataflow("Emma Dataflow", sinks.toList)
  }
}