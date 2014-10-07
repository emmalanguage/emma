package eu.stratosphere.emma.runtime

import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

class OpTest extends AssertionsForJUnit {

  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{currentMirror => cm}
  import scala.tools.reflect.ToolBox

  import eu.stratosphere.emma.ir._

  val tb = cm.mkToolBox()

  @Test def testMap(): Unit = {
  }
}
