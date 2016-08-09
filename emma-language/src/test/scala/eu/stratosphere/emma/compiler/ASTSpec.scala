package eu.stratosphere
package emma.compiler

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ASTSpec extends BaseCompilerSpec {

  import compiler._
  object Bal

  "method calls should" - {
    "resolve overloaded symbols" in {
      val seq = api.Type.check(u.reify { Seq }.tree)
      val fill = api.Type[Seq.type].member(api.TermName("fill")).asTerm
      val examples = api.Type.check(u.reify {(
        Seq.fill(1)('!'),
        Seq.fill(1, 2)('!'),
        Seq.fill(1, 2, 3)('!')
      )}.tree).children.tail

      for ((dim, exp) <- Seq(Seq(1), Seq(1, 2), Seq(1, 2, 3)) zip examples) {
        val argss = Seq(dim.map(api.Lit(_)), Seq(api.Lit('!')))
        val act = api.DefCall(Some(seq))(fill, api.Type.char)(argss: _*)
        act shouldBe alphaEqTo (exp)
      }
    }
  }

  "static objects should" - {
    "be unqualified" in {
      val bar = u.rootMirror.staticModule("org.example.Foo.Bar")
      val ref = api.ModuleRef(bar)
      val qual = api.Tree.resolveStatic(bar)
      unQualifyStaticModules(qual) shouldBe alphaEqTo (ref)
      qualifyStaticModules(ref) shouldBe alphaEqTo (qual)
    }
  }

  "dynamic objects should" - {
    "remain qualified" in {
      val bal = api.Type.check(u.reify { Bal }.tree)
      val ref = api.TermRef(api.TermSym.of(bal))
      val act = unQualifyStaticModules(bal)
      act should not be alphaEqTo (ref)
    }
  }
}
