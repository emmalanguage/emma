package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.runtime.{Native, Flink}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import eu.stratosphere.emma.runtime

@RunWith(classOf[JUnitRunner])
class TicTacToeTest extends FlatSpec with Matchers {

  "TicTacToe" should "compute the game-theoretical values" in {
    val rt = runtime.default()

    val rtName = System.getProperty("emma.execution.backend", "")
    if (rtName == "flink" || rtName == "native") {
      val result = new TicTacToe().algorithm.run(rt).fetch()

      // Could be compared to the native rt, but it is too slow, so I calculate a "checksum" instead.

      result.exists(v => v.vc.isInstanceOf[TicTacToe.Undefined]) should equal(false)

      result.flatMap(v => v.vc match {
        case v: TicTacToe.Win => Seq(v.depth);
        case _ => Seq()
      }).sum should equal(8697)

      result.flatMap(v => v.vc match {
        case v: TicTacToe.Loss => Seq(v.depth);
        case _ => Seq()
      }).sum should equal(4688)

      result.flatMap(v => v.vc match {
        case v: TicTacToe.Count => Seq(v.count);
        case _ => Seq()
      }).sum should equal(3495)
    } else {
      println("Skipping TicTacToe test, because it only works with Flink and Native. (because of the stateful)")
    }
  }
}
