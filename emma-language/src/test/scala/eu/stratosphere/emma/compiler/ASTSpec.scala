package eu.stratosphere
package emma.compiler

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ASTSpec extends BaseCompilerSpec {

  import compiler._
  import universe._

  "method calls" - {
    "overloaded" in {
      val seq = Type.check(q"scala.collection.Seq")
      val fill = Type.of(seq).member(Term.name("fill")).asTerm
      for (dim <- Seq(1 :: Nil, 1 :: 2 :: Nil, 1 :: 2 :: 3 :: Nil)) {
        val act = Method.call(seq, fill, Type.char)(dim.map(Term.lit(_)), Term.lit('!') :: Nil)
        val exp = Type.check(q"scala.collection.Seq.fill(..$dim)('!')")
        act shouldBe alphaEqTo(exp)
      }
    }
  }

  "refs should be unqualified" - {

    "for static objects" in {
      val Foo = Tree.resolve(rootMirror.staticModule("org.example.Foo"))
      val Bar = Type.of(Foo).member(Term.name("Bar")).asTerm
      val ref = Term ref Bar
      val sel = Term sel (Foo, Bar)
      unQualifyStaticModules(sel) shouldBe alphaEqTo(ref)
      qualifyStaticModules(ref) shouldBe alphaEqTo(sel)
    }
  }

  "refs should mot be unqualified" - {

    "for non-static objects" in {
      val bal = typeCheck(reify { Bal })
      val exp = Term ref Term.sym(bal)
      val act = unQualifyStaticModules(bal)
      act should (not be alphaEqTo(exp))
    }
  }

  object Bal
}
