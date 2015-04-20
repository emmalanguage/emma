package ${package}

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.runtime

object Job {
  def main(args: Array[String]): Unit = {
    val alg = emma.parallelize {
      val db = for (x <- DataBag(Seq(1, 2, 3))) yield x
    }

    alg.run(runtime.Native())
    //alg.run(runtime.factory("flink-local", "localhost", 6123))
    //alg.run(runtime.factory("spark-local", "local[*]", 6123))
  }
}