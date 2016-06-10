package eu.stratosphere.emma.compiler.lang

import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StringifyTests extends BaseCompilerSpec {
  import compiler._
  import universe._

  def `type-check and extract from`(tree: Tree) =
    Type.check(tree) match {
      case Apply(_, args) => args
      case Block(stats, _) => stats
    }

  "literals" - {
    val examples = `type-check and extract from`(reify(
      42, 42L, 3.14, 3.14F, .1e6, 'c', "string"
    ).tree)

    assert(examples.map(compiler.prettyPrint(_)).mkString(", ") ==
      "42, 42L, 3.14, 3.14, 100000.0, 'c', \"string\"")
  }

  "lambdas" - {
    val examples = `type-check and extract from`(reify({
      val a = 3
      val f = (v : Int) => v + v
      f(a)
    }).tree)

    assert(examples.map(compiler.prettyPrint(_)).mkString("\n") ==
      """val a = 3
val f = (v : Int) => v.$plus(v)
f.apply(a)
      """)
  }

  "expressions" - {
    val examples = `type-check and extract from`(reify({
      val a = 3
      val b = a + 5
    }).tree)

    assert(examples.map(compiler.prettyPrint(_)).mkString("\n") ==
      "val a = 3\nval b = a.$plus(5)")
  }
}
