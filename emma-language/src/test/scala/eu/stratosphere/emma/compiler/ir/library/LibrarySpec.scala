package eu.stratosphere.emma.compiler.ir.library

import eu.stratosphere.emma.api.{CSVOutputFormat, TextInputFormat, _}
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import eu.stratosphere.emma.compiler.ir.library.LibrarySpec.PartialUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LibrarySpec extends BaseCompilerSpec {
  import compiler._
  import universe._

  def withTypeCheckAndOwnerRepair[T]: (Tree => Tree) => Expr[T] => Tree = { f =>
    { (_: Expr[T]).tree } andThen
    { Type.check(_) } andThen
    { f } andThen
    { Owner.at(Owner.enclosing) }
  }

  def typeCheckAndBetaReduce[T]: Expr[T] => Tree = withTypeCheckAndOwnerRepair {
    { Core.resolveNameClashes(_) } andThen
    { time(betaReduce(_), "ß-reducing") }
  }

  def typeCheckOnly = withTypeCheckAndOwnerRepair { identity }

  def typeCheckAndInlineOnce = withTypeCheckAndOwnerRepair { transformApplications }

  def typeCheckInlineBetaReduce = withTypeCheckAndOwnerRepair {
    { time(transformInline(_), "inlining") } andThen
    { betaReduce }
  }

  "The @emma.inline annotation" - {
    val singletonMembers = List("plus")
    val inlineSingletonMembers = singletonMembers.map { _ + "__expr$1"}
    val overloadedMembers = List("geq", "lt", "between")
    val inlinedOverloadedMembers = for {
      ol <- overloadedMembers
      no <- 1 to 2
    } yield s"${ol}__expr$$${no}"

    val expectedInlineMembers = inlineSingletonMembers ::: inlinedOverloadedMembers
    "should extend all methods if annottee is a module" in {
      for (genMember <- expectedInlineMembers; tn = TermName(genMember))
        typeOf[LibrarySpec.Util.type].member(tn).isMethod shouldBe true
    }
    "should extend only one method if annottee is a method " in {
      val partialInlineModuleType = typeOf[PartialUtils.type]
      val expectedMember = TermName("hello__expr$1")
      partialInlineModuleType.member(expectedMember).isMethod shouldBe true
      val nonExpectedMember = TermName("someMethod__expr$1") // should not exist
      partialInlineModuleType.member(nonExpectedMember).isMethod shouldBe false
    }
  }
  "Library Compiler" - {
    "can perform ß-reduction" in {
      val act = typeCheckAndBetaReduce(reify {
        val lo = 23
        val hi = 66
        val between = ((lo: Int, hi: Int) => (x: Int) => lo <= x && x < hi)(lo, hi)(42)
      })
      val exp = typeCheckOnly(reify {
        val lo = 23
        val hi = 66
        val between = lo <= 42 && 42 < hi
      })
      act shouldBe alphaEqTo(exp)
    }
    "can reflect and execute a reified method" in {
      val mir = tb.mirror
      val utilSymbol = mir.staticModule("eu.stratosphere.emma.compiler.ir.library.LibrarySpec.Util")
      val moduleMirror = mir.reflectModule(utilSymbol)
      val moduleInstanceMirror = mir.reflect(moduleMirror.instance)
      val methodSymbol = utilSymbol.info.decl(TermName("lt__expr$1")).asMethod
      val methodMirror = moduleInstanceMirror.reflectMethod(methodSymbol)
      methodMirror(universe) match {
        case e: Expr[_] => Type.check(e.tree)
        case x => fail(s"Not an expression: $x")
      }
    }
    "should splice in until a fixpoint is reached" in {
      val act = typeCheckInlineBetaReduce(reify {
        val lo = 23
        val hi = 66
        val x = 42
        val isBetween = LibrarySpec.Util.between(lo, hi)(x)
        val loD = 23.0
        val hiD = 66.0
        val xD = 42.0
        val isBetweenD = LibrarySpec.Util.between(loD, hiD)(xD)
      })
      val exp = typeCheckOnly(reify {
        val lo = 23
        val hi = 66
        val x = 42
        val isBetween = lo <= x && x < hi
        val loD = 23.0
        val hiD = 66.0
        val xD = 42.0
        val isBetweenD = loD <= xD && xD < hiD
      })
      act shouldBe alphaEqTo(exp)
    }
    "should work in a comprehensions" in {
      val act = typeCheckInlineBetaReduce(reify {
        val xs = for {
          x <- DataBag(1 to 1000) if LibrarySpec.Util.between(500, 600)(x)
        } yield x
      })
      val exp = typeCheckOnly(reify{
        val xs = DataBag(1 to 1000)
          .withFilter(x => 500 <= x && x < 600)
          .map(x => x)
      })
      act shouldBe alphaEqTo(exp)
    }
    "should splice using the method-only annotations" in {
      val act = typeCheckInlineBetaReduce(reify {
        val hw = PartialUtils.hello("world")
      })
      val exp = typeCheckOnly(reify {
        val hw = StringContext("", " ", "").s("Hello", "world")
      })
      act shouldBe alphaEqTo(exp)
    }
    "should work on the classic WordCount example" in {
      import LibrarySpec.PartialUtils._
      val inPath = "/some/Path/"
      val outPath = "some/otherPath"
      val act = typeCheckInlineBetaReduce(reify {
        // read the input files and split them into lowercased words
        val words = loadWords(inPath)
        // group the words by their identity and count the occurrence of each word
        val counts = for {
          group <- words.groupBy[String] { identity }
        } yield (group.key, group.values.size)

        // write the results into a CSV file
        write(outPath,new CSVOutputFormat[(String, Long)])(counts)
      })
      val exp = typeCheckOnly(reify {
        // read the input files and split them into lowercased words
        val words = for {
          line <- read(inPath, new TextInputFormat[String]('\n'))
          word <- DataBag[String](line.toLowerCase.split("\\W+"))
        } yield word

        // group the words by their identity and count the occurrence of each word
        val counts = for {
          group <- words.groupBy[String] { identity }
        } yield (group.key, group.values.size)

        // write the results into a CSV file
        write(outPath,new CSVOutputFormat[(String, Long)])(counts)
      })
      act shouldBe alphaEqTo(exp)
    }
  }
}

object LibrarySpec {
  @emma.inline object Util {
    def geq(lower: Int)(i: Int) = lower <= i

    def lt(upper: Int)(i: Int) = i < upper

    //  @RuntimeAnnotationForwardToThis(func=plus__expr) // or string // showCode // rekursiv einsplicen
    def between(lower: Int, upper: Int)(x: Int): Boolean =
      geq(lower)(x) && lt(upper)(x)

    def geq(lower: Double)(x: Double) = lower <= x

    def lt(upper: Double)(x: Double) = x < upper

    def between(lower: Double, upper: Double)(x: Double): Boolean =
      geq(lower)(x) && lt(upper)(x)

    def plus[T: Numeric](i: T, j: T) = {
      // (implicit n:Numeric[T])
      val n = implicitly[Numeric[T]]
      n.plus(i, j)
    }
  }

  object PartialUtils {
    def someMethod: String = "Hello World"
    @emma.inline def salute(): String = "Hello"
    @emma.inline def hello(s: String): String = s"${salute()} $s"
    @emma.inline def loadWords(inPath: String): DataBag[String] = {
      for {
        line <- read(inPath, new TextInputFormat[String]('\n'))
        word <- DataBag[String](splitByWords(line))
      } yield word
    }
    @emma.inline def wordSeparator(): String = "\\W+"
    @emma.inline def splitByWords(line: String) = line.toLowerCase.split(wordSeparator())
  }
}