package eu.stratosphere.emma.examples.prototype

import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.runtime.Engine

object TranslationPrototype {

  /**
   * Temporary, only for debugging.
   *
   */
  def main(args: Array[String]): Unit = {
    val prototype = new TranslationPrototype(runtime.factory("flink-local", "localhost", 6123))
    prototype.run()
  }
}

class TranslationPrototype(rt: Engine) extends Algorithm(rt) {
  import eu.stratosphere.emma.api._

  def run() = {
    emma.parallelize {

      val a = for (
        x <- DataBag(1 to 10);
        y <- DataBag(11 to 20);
        z <- DataBag(Seq(1, 2, 3));
        if x < 5 && y > 15 && z == 2 && z > 2 && y == 20) yield (x, y, z)

      val b = for (
        x <- DataBag(1 to 10);
        y <- DataBag(11 to 20);
        z <- DataBag(Seq(1, 2, 3));
        if x < 5; if y > 15; if z == 2; if z > 2; if y == 20) yield (x, y, z)

      //val b = DataBag(Seq(1, 2, 3)).map(x => x).withFilter(x => x < 5).map(y => y)
    }
  }

}