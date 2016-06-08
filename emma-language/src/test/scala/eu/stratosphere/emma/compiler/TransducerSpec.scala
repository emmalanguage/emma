package eu.stratosphere
package emma.compiler

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import cats.implicits._
import shapeless.HNil

@RunWith(classOf[JUnitRunner])
class TransducerSpec extends BaseCompilerSpec {

  import compiler._
  import universe._
  import Attr._

  val example = reify {
    val x1 = 1
    val x2 = {
      val y1 = 2
      val y2 = 3
      y1 + y2
    }
    val x3 = {
      val y3 = 4
      val y4 = {
        val z1 = 5
        val z2 = 6
        z1 + z2
      }
      y3 + y4
    }
    x1 + x2 + x3
  }.tree

  "correct order of" - {
    "top-down traversal" in {
      var vals = Vector.empty[String]
      TopDown.traverse(at {
        case ValDef(_, name, _, _) => vals :+= name.toString
      })(example)

      val collected = TopDown.accumulate(collect[Vector, String, HNil] {
        case (ValDef(_, name, _, _), _) => name.toString
      }).traverse(at.any)(example)._1.head

      vals should contain theSameElementsInOrderAs collected
      vals should contain theSameElementsInOrderAs
        Vector("x1", "x2", "y1", "y2", "x3", "y3", "y4", "z1", "z2")
    }

    "top-down-break traversal" in {
      var vals = Vector.empty[String]
      TopDown.break.traverse(at {
        case ValDef(_, name, _, _) => vals :+= name.toString
      })(example)

      val collected = TopDown.break.accumulate(collect[Vector, String, HNil] {
        case (ValDef(_, name, _, _), _) => name.toString
      }).traverse(at { case _: ValDef => })(example)._1.head

      vals should contain theSameElementsInOrderAs collected
      vals should contain theSameElementsInOrderAs Vector("x1", "x2", "x3")
    }

    "bottom-up traversal" in {
      var vals = Vector.empty[String]
      BottomUp.traverse(at {
        case ValDef(_, name, _, _) => vals :+= name.toString
      })(example)

      val collected = BottomUp.accumulate(collect[Vector, String, HNil] {
        case (ValDef(_, name, _, _), _) => name.toString
      }).traverse(at.any)(example)._1.head

      vals should contain theSameElementsInOrderAs collected
      vals should contain theSameElementsInOrderAs
        Vector("x1", "y1", "y2", "x2", "y3", "z1", "z2", "y4", "x3")
    }

    "bottom-up-break traversal" in {
      var vals = Vector.empty[String]
      BottomUp.break.traverse(at {
        case ValDef(_, name, _, _) => vals :+= name.toString
      })(example)

      val collected = BottomUp.break.accumulate(collect[Vector, String, HNil] {
        case (ValDef(_, name, _, _), _) => name.toString
      }).traverse(at { case _: ValDef => })(example)._1.head

      vals should contain theSameElementsInOrderAs collected
      vals should contain theSameElementsInOrderAs
        Vector("x1", "y1", "y2", "y3", "z1", "z2")
    }
  }

  "accumulation" - {

  }
}
