package ${package}

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.runtime

object Job {
  def main(args: Array[String]): Unit = {
    val alg = emma.parallelize {
      val db = for (x <- DataBag(Seq(1, 2, 3))) yield x
    }

    alg.run(runtime.factory("native") // one of "native", "flink", or "spark"
  }
}