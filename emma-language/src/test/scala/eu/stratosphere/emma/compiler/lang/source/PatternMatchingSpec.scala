package eu.stratosphere.emma
package compiler.lang.source

import compiler.BaseCompilerSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for the `Core.foreach2loop` transformation. */
@RunWith(classOf[JUnitRunner])
class PatternMatchingSpec extends BaseCompilerSpec {

  import compiler._
  import PatternMatchingSpec._

  val destructPatternMatchesPipeline: u.Expr[Any] => u.Tree =
    compiler.pipeline(typeCheck = true, withPre = false)(
      fixLambdaTypes,
      unQualifyStaticModules,
      normalizeStatements,
      tree => time(PatternMatching.destruct(tree), "destruct irrefutable pattern matches")
    ).compose(_.tree)

  "pattern match" - {
    "with ref lhs" in {

      val dogs = (Dog("Max"), Dog("Gidget"))

      val act = destructPatternMatchesPipeline(u.reify {
        val nameFst = dogs match {
          case pair@(Dog(name), _) => name
        }

        val nameSnd = dogs match {
          case (_, snd@Dog(name)) => name
        }

        nameFst == nameSnd
      })

      val exp = idPipeline(u.reify {
        val nameFst = {
          val pair = dogs
          val name = pair._1.name
          name
        }

        val nameSnd = {
          val snd = dogs._2
          val name = snd.name
          name
        }

        nameFst == nameSnd
      })

      act shouldBe alphaEqTo (exp)
    }

    "with complex lhs" in {

      val act = destructPatternMatchesPipeline(u.reify {
        val nameFst = (Dog("Max"), Dog("Gidget")) match {
          case pair@(Dog(name), _) => name
        }

        val nameSnd = (Dog("Max"), Dog("Gidget")) match {
          case (_, snd@Dog(name)) => name
        }

        nameFst == nameSnd
      })

      val exp = idPipeline(u.reify {
        val nameFst = {
          val x$1 = (Dog("Max"), Dog("Gidget"))
          val pair = x$1
          val name = pair._1.name
          name
        }

        val nameSnd = {
          val x$2 = (Dog("Max"), Dog("Gidget"))
          val snd = x$2._2
          val name = snd.name
          name
        }

        nameFst == nameSnd
      })

      act shouldBe alphaEqTo (exp)
    }
  }
}

object PatternMatchingSpec {

  sealed trait Animal {
    def name: String
  }

  case class Cat(name: String) extends Animal
  case class Dog(name: String) extends Animal
  case class Hawk(name: String) extends Animal
}
