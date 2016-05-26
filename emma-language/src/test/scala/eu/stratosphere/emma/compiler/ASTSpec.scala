package eu.stratosphere
package emma.compiler

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ASTSpec extends BaseCompilerSpec {

  import compiler._
  import universe._
  import Term._

  "method calls" - {
    "overloaded" in {
      val seq = Type.check(q"scala.collection.Seq")
      val fill = Type.of(seq).member(Term.name("fill")).asTerm
      for (dim <- Seq(1 :: Nil, 1 :: 2 :: Nil, 1 :: 2 :: 3 :: Nil)) {
        val act = Method.call(seq, fill, Type.char)(dim.map(lit(_)), lit('!') :: Nil)
        val exp = Type.check(q"scala.collection.Seq.fill(..$dim)('!')")
        act shouldBe alphaEqTo(exp)
      }
    }
  }
}
