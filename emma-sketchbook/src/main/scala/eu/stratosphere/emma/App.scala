package eu.stratosphere.emma

object App {

  def main(args: Array[String]) {

    val X1 = dataflow {
      val A = read("file:///tmp/emma/A.txt", new InputFormat[Int])
      val B = join[Int, Int, Int](a => a, a => a)(A, A).map(x => x._1 + x._2).withFilter(_ > 7)

      write("file:///tmp/emma/C.txt", new OutputFormat[(Int)])(B)
    }

    val X2 = reference()

    println("// plan")
    X1.print()
    println("")
  }

  def reference() = {

    import _root_.eu.stratosphere.emma.mc._
    import _root_.scala.collection.mutable.ListBuffer
    import _root_.scala.reflect.runtime.universe._

    val sinks = ListBuffer[Comprehension[AlgExists, Boolean, Boolean]]()

    val _MC_00000_C = {
      val _MC_00001_B = {
        val x = null.asInstanceOf[Int]
        val y = null.asInstanceOf[Int]

        val _MC_00002_A = {
          val bind_bytes = DirectGenerator.apply[Seq[Byte], List[Seq[Byte]]]("bytes", {
            val ifmt = new InputFormat[Int]()
            val dop = 20

            reify {
              ifmt.split("file:///tmp/emma/A.txt", dop)
            }
          })

          val head = Head[Seq[Byte], Seq[Int]]({
            val ifmt = new InputFormat[Int]()
            val bytes = null.asInstanceOf[Seq[Byte]]

            reify {
              ifmt.read(bytes)
            }
          })

          Comprehension[AlgBag[Int], Int, List[Int]](head, bind_bytes)
        }

        val _MC_00003_A = {
          val bind_bytes = DirectGenerator.apply[Seq[Byte], List[Seq[Byte]]]("bytes", {
            val ifmt = new InputFormat[Int]()
            val dop = 20

            reify {
              ifmt.split("file:///tmp/emma/A.txt", dop)
            }
          })

          val head = Head[Seq[Byte], Seq[Int]]({
            val ifmt = new InputFormat[Int]()
            val bytes = null.asInstanceOf[Seq[Byte]]

            reify {
              ifmt.read(bytes)
            }
          })

          Comprehension[AlgBag[Int], Int, List[Int]](head, bind_bytes)
        }

        val qualifiers = ListBuffer[Qualifier]()
        qualifiers += ComprehensionGenerator[AlgBag[Int], Int, List[Int]]("x", _MC_00002_A)
        qualifiers += ComprehensionGenerator[AlgBag[Int], Int, List[Int]]("y", _MC_00003_A)

        val head = Head[Int, Int](reify {
          3 * x * y
        })

        Comprehension[AlgBag[Int], Int, List[Int]](head, qualifiers.toList)
      }

      val bind_record = ComprehensionGenerator[AlgBag[Int], Int, List[Int]]("record", _MC_00001_B)
      val head = Head[Int, Seq[Boolean]]({
        val ofmt = new OutputFormat[Int]()
        val record = null.asInstanceOf[Int]

        reify {
          ofmt.write(record)
        }
      })

      Comprehension[AlgExists, Boolean, Boolean](head, bind_record)
    }

    sinks += _MC_00000_C

    Dataflow("Emma Dataflow", sinks.toList)
  }
}
