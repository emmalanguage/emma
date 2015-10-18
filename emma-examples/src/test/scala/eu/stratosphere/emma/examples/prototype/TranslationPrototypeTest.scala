package eu.stratosphere.emma.examples.prototype

import eu.stratosphere.emma.testutil._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class TranslationPrototypeTest extends FlatSpec with Matchers with BeforeAndAfter {
  "TranslationPrototype" should "compile and run" in withRuntime() { rt =>
    new TranslationPrototype(rt).run()
  }
}
