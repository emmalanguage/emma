package eu.stratosphere.emma.examples.prototype

import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSuite, Matchers}

class TranslationPrototype(rt: runtime.Engine) extends Algorithm(rt) {

  def run() = {
  }
}

object TranslationPrototype {
}

@RunWith(classOf[JUnitRunner])
class TranslationPrototypeTest extends FunSuite with PropertyChecks with Matchers {

  val rt = runtime.Native()

  test("execute translation prototype") {
    new TranslationPrototype(rt).run()
  }
}