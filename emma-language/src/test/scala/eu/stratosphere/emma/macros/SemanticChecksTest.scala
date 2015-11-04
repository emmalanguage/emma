package eu.stratosphere.emma.macros

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.tools.reflect.ToolBoxError

@RunWith(classOf[JUnitRunner])
class SemanticChecksTest extends FlatSpec with Matchers with RuntimeUtil {
  import SemanticChecksTest.Foo

  lazy val tb = mkToolBox()
  import universe._

  val imports =
    q"import _root_.eu.stratosphere.emma.api._" ::
    q"import _root_.eu.stratosphere.emma.ir._" ::
    q"import _root_.eu.stratosphere.emma.macros.SemanticChecksTest.Foo" :: Nil

  "The updateWith* functions" should
    "throw an exception when the stateful is accessed from the updateWith* UDF" in {
      val bag = DataBag(Foo(1, 1) :: Nil)

      // these should throw exceptions:

      intercept[StatefulAccessedFromUdfException] {
        val withState = stateful[Foo, Int](bag)
        withState updateWithZero { _ => withState.bag() }
      }

      val updates = DataBag(Foo(1, 2) :: Nil)

      intercept[StatefulAccessedFromUdfException] {
        val withState = stateful[Foo, Int](bag)
        withState.updateWithOne(updates)(_.id, (_, _) => withState.bag())
      }

      intercept[StatefulAccessedFromUdfException] {
        val withState = stateful[Foo, Int](bag)
        withState.updateWithMany(updates)(_.id, (_, _) => withState.bag())
      }

      // and these should not throw exceptions:
      val withState = stateful[Foo, Int](bag)
      withState.updateWithZero { _ => DataBag() }
      withState.updateWithOne(updates)(_.id, (_, _) => DataBag())
      withState.updateWithMany(updates)(_.id, (_, _) => DataBag())
    }

  "parallelize" should "check for the stateful being accessed from the updateWith* UDF" in {
    val bagDef = q"val bag = DataBag(Foo(1, 1) :: Nil)"
    val updDef = q"val updates = DataBag(Foo(1, 2) :: Nil)"

    q"""{
      $bagDef
      emma.parallelize {
        val withState = stateful[Foo, Int](bag)
        withState updateWithZero { _ => withState.bag() }
      }
    }""" should failWith[StatefulAccessedFromUdfException]

    q"""{
      $bagDef
      $updDef
      emma.parallelize {
        val withState = stateful[Foo, Int](bag)
        withState.updateWithOne(updates)(_.id, (_, _) => withState.bag())
      }
    }""" should failWith[StatefulAccessedFromUdfException]

    q"""{
      $bagDef
      $updDef
      emma.parallelize {
        val withState = stateful[Foo, Int](bag)
        withState.updateWithMany(updates)(_.id, (_, _) => withState.bag())
      }
    }""" should failWith[StatefulAccessedFromUdfException]

    // this should compile
    val bag = DataBag(Foo(1, 1) :: Nil)
    emma.parallelize {
      val withState = stateful[Foo, Int](bag)
      val updates = DataBag(Foo(1, 2) :: Nil)
      withState.updateWithZero { _ => DataBag() }
      withState.updateWithOne(updates)(_.id, (_, _) => DataBag())
      withState.updateWithMany(updates)(_.id, (_, _) => DataBag())
    }
  }

  def failWith[E <: Throwable : TypeTag] =
    include (typeOf[E].typeSymbol.name.toString) compose { (tree: Tree) =>
      intercept[ToolBoxError] { tb.typecheck(q"{ ..$imports; $tree }") }.getMessage
    }
}

object SemanticChecksTest {
  case class Foo(@id id: Int, var n: Int) extends Identity[Int] {
    override def identity = id
  }
}
