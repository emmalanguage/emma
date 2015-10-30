package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.testutil.withRuntime

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TicTacToeTest extends FlatSpec with Matchers {
  import TicTacToe._

  "TicTacToe" should "compute the game-theoretical values" in withRuntime() { rt =>
    val rtName = sys.props.getOrElse("emma.execution.backend", "").toLowerCase
//    if (rtName == "flink" || rtName == "native") {
      val result = new TicTacToe().algorithm.run(rt).fetch().map { _.vc }
      // Could be compared to the native rt, but it is too slow, so we use a "checksum" instead
      result.collect { case u: Undefined => u } should be ('empty)
      result.collect { case Win(depth) => depth }.sum should equal (8697)
      result.collect { case Loss(depth) => depth }.sum should equal (4688)
      result.collect { case Count(cnt) => cnt }.sum should equal (3495)
//    } else {
//      println("""
//        |Skipping TicTacToe test, because it only works with Flink and Native.
//        |(due to stateful API support)
//        |""".stripMargin)
//    }
  }
}
