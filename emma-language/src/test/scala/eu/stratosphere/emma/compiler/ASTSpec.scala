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

  "refs should be unqualified" - {

    "for static methods" in {
      val foo = Tree.resolve(rootMirror.staticModule("org.example.foo.package"))
      val bar = Type.of(foo).member(Term.name("bar")).asTerm
      val ref = Term ref bar

      val sel = Term sel (foo, bar)

      unqualifyStaticSels(sel) shouldBe alphaEqTo(ref)
      qualifyStaticRefs(ref) shouldBe alphaEqTo(sel)
    }

    "for static objects" in {
      val Foo = Tree.resolve(rootMirror.staticModule("org.example.Foo"))
      val Bar = Type.of(Foo).member(Term.name("Bar")).asTerm
      val ref = Term ref Bar
      val sel = Term sel (Foo, Bar)
      unqualifyStaticSels(sel) shouldBe alphaEqTo(ref)
      qualifyStaticRefs(ref) shouldBe alphaEqTo(sel)
    }
  }

  "refs should mot be unqualified" - {

    "for non-static objects" in {
      val bal = typeCheck(reify { Bal })
      val exp = Term ref Term.sym(bal)
      val act = unqualifyStaticSels(bal)
      act should (not be alphaEqTo(exp))
    }
  }

  object Bal
}
