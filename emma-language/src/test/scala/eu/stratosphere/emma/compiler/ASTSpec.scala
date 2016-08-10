package eu.stratosphere
package emma.compiler

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ASTSpec extends BaseCompilerSpec {

  import compiler._
  import universe._

  object Bal

  "method calls should" - {
    "resolve overloaded symbols" in {
      val seq = api.Type.check(q"scala.collection.Seq")
      val fill = api.Type.of(seq).member(api.TermName("fill")).asTerm
      for (dim <- Seq(Seq(1), Seq(1, 2), Seq(1, 2, 3))) {
        val argss = Seq(dim.map(api.Lit(_)), Seq(api.Lit('!')))
        val act = api.DefCall(Some(seq))(fill, api.Type.char)(argss: _*)
        val exp = api.Type.check(q"scala.collection.Seq.fill(..$dim)('!')")
        act shouldBe alphaEqTo (exp)
      }
    }
  }

  "static objects should" - {
    "be unqualified" in {
      val Foo = api.Tree.resolveStatic(rootMirror.staticModule("org.example.Foo"))
      val Bar = api.Type.of(Foo).member(api.TermName("Bar")).asModule
      val ref = api.ModuleRef(Bar)
      val qual = api.Tree.resolveStatic(rootMirror.staticModule("org.example.Foo.Bar"))
      unQualifyStaticModules(qual) shouldBe alphaEqTo (ref)
      qualifyStaticModules(ref) shouldBe alphaEqTo (qual)
    }
  }

  "dynamic objects should" - {
    "remain qualified" in {
      val bal = typeCheck(reify { Bal })
      val ref = api.TermRef(api.TermSym.of(bal))
      val act = unQualifyStaticModules(bal)
      act should not be alphaEqTo (ref)
    }
  }
}
