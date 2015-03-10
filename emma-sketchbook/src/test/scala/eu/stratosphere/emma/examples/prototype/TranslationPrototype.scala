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

  def run() = {
  }

}