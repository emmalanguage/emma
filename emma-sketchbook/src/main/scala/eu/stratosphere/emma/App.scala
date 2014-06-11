package eu.stratosphere.emma

object App {

  def main(args: Array[String]) {

    val dataflow01 = dataflow {

      val A = read("hdf://tmp/files/A.csv", new InputFormat[(Int, Int, String, String)]);
      val B = for (a <- A; if a._1 + a._2 < 42) yield a._3
      val C = write("file:///tmp/emma/C.txt", new OutputFormat[(String)])(B)

      C
    }

    val dataflow02 = reference()

    println("// plan")
    dataflow01.print()
    println("")
//    println("// plan")
//    dataflow02.print()
//    println("")
  }

  def reference() = {

    import _root_.eu.stratosphere.emma.mc._
    import _root_.scala.collection.mutable.ListBuffer
    import _root_.scala.reflect.runtime.universe._

    val sinks = ListBuffer[Comprehension]()

    val _MC_00000_C = {
      val _MC_00001_B = {
        val x = null.asInstanceOf[Int]
        val y = null.asInstanceOf[Int]

        val _MC_00002_A = {
          val bind_bytes = ScalaExprGenerator("bytes", ScalaExpr({
            val ifmt = new InputFormat[Int]()
            val dop = null.asInstanceOf[Int]

            reify {
              ifmt.split("file:///tmp/emma/A.txt", dop)
            }
          }))

          val bind_record = ScalaExprGenerator("record", ScalaExpr({
            val ifmt = new InputFormat[Int]()
            val bytes = null.asInstanceOf[Seq[Byte]]

            reify {
              ifmt.read(bytes)
            }
          }))

          val head = ScalaExpr({
            val record = null.asInstanceOf[Int]

            reify {
              record
            }
          })

          Comprehension(monad.Bag[Int], head, bind_bytes, bind_record)
        }

        val _MC_00003_A = {
          val bind_bytes = ScalaExprGenerator("bytes", ScalaExpr({
            val ifmt = new InputFormat[Int]()
            val dop = null.asInstanceOf[Int]

            reify {
              ifmt.split("file:///tmp/emma/A.txt", dop)
            }
          }))

          val bind_record = ScalaExprGenerator("record", ScalaExpr({
            val ifmt = new InputFormat[Int]()
            val bytes = null.asInstanceOf[Seq[Byte]]

            reify {
              ifmt.read(bytes)
            }
          }))

          val head = ScalaExpr({
            val record = null.asInstanceOf[Int]

            reify {
              record
            }
          })

          Comprehension(monad.Bag[Int], head, bind_bytes, bind_record)
        }

        val qualifiers = ListBuffer[Qualifier]()
        qualifiers += ComprehensionGenerator("x", _MC_00002_A)
        qualifiers += ComprehensionGenerator("y", _MC_00003_A)

        val head = ScalaExpr(reify {
          (x, y)
        })

        Comprehension(monad.Bag[(Int, Int)], head, qualifiers.toList)
      }

      val bind_record = ComprehensionGenerator("record", _MC_00001_B)
      val head = ScalaExpr({
        val ofmt = new OutputFormat[(Int, Int)]()
        val record = null.asInstanceOf[(Int, Int)]

        reify {
          ofmt.write(record)
        }
      })

      Comprehension(monad.All, head, bind_record)
    }

    sinks += _MC_00000_C

    Dataflow("Emma Dataflow", sinks.toList)
  }
}
