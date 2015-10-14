package eu.stratosphere.emma.macros

import eu.stratosphere.emma.ir.Filter

import tools.reflect.ToolBox
import org.junit.Ignore
import org.junit.Test

class PredicateNormalizationTest {

  @Ignore
  @Test
  def `Normalize Predicates into CNF`() {
    val cm = reflect.runtime.currentMirror
    val tb = mkToolbox(s"-cp ${toolboxClasspath}")
    val tree = tb.parse(
      """
        |import eu.stratosphere.emma.api._
        |import eu.stratosphere.emma.ir._
        |
        |
        |eu.stratosphere.emma.macros.testmacros.parallelize {
        | val alg = for (x <- DataBag(1 to 10)
        |                if !(x > 5 || (x < 2 && x == 0)) || (x > 5 || !(x < 2)))
        |              yield x} """.stripMargin)

    val tree1 = tb.typecheck(tree)
    import tb.u._

    val filters = tree1 collect {case f@Apply(TypeApply(Select(Select(q, TermName("Filter")), _), as), args)=> f}
    filters.size mustBe 1

    val ft = filters.flatMap(x => x collect {case Literal(t) => t})
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.<(2).`unary_!`.||(x.==(0).`unary_!`).||(x.>(5))))")) mustBe true
  }

  @Ignore
  @Test
  def `seperate filter conjunction into seperate filters`() {
    val cm = reflect.runtime.currentMirror
    val tb = mkToolbox(s"-cp ${toolboxClasspath}")
    val tree = tb.parse(
      """
        |import eu.stratosphere.emma.api._
        |import eu.stratosphere.emma.ir._
        |
        |
        |eu.stratosphere.emma.macros.testmacros.parallelize {
        | val alg = for (x <- DataBag(1 to 10)
        |                if x > 5 && x > 0 && x != 4)
        |              yield x} """.stripMargin)

    val tree1 = tb.typecheck(tree)
    import tb.u._

    val filters = tree1 collect {case f@Apply(TypeApply(Select(Select(q, TermName("Filter")), _), as), args)=> f}
    filters.size mustBe 3

    val ft = filters.flatMap(x => x collect {case Literal(t) => t})
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.!=(4)))")) mustBe true
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.>(5)))")) mustBe true
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.>(0)))")) mustBe true
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.>(5).&&(x.>(0)).&&(x.!=(4))))")) mustBe false
  }

  @Ignore
  @Test
  def `seperate filter conjunction and single predicate into seperate filters`() {
    val cm = reflect.runtime.currentMirror
    val tb = mkToolbox(s"-cp ${toolboxClasspath}")
    val tree = tb.parse(
      """
        |import eu.stratosphere.emma.api._
        |import eu.stratosphere.emma.ir._
        |
        |
        |eu.stratosphere.emma.macros.testmacros.parallelize {
        | val alg = for (x <- DataBag(1 to 10)
        |                if x > 5 && x > 0 && x != 4 if x <= 2)
        |              yield x} """.stripMargin)

    val tree1 = tb.typecheck(tree)
    import tb.u._

    val filters = tree1 collect {case f@Apply(TypeApply(Select(Select(q, TermName("Filter")), _), as), args)=> f}
    filters.size mustBe 4

    val ft = filters.flatMap(x => x collect {case Literal(t) => t})
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.!=(4)))")) mustBe true
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.>(5)))")) mustBe true
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.>(0)))")) mustBe true
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.<=(2)))")) mustBe true
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.>(5).&&(x.>(0)).&&(x.!=(4))))")) mustBe false
  }

  @Ignore
  @Test
  def `seperate filter conjunction with udf predicate into seperate filters`() {
    val cm = reflect.runtime.currentMirror
    val tb = mkToolbox(s"-cp ${toolboxClasspath}")
    val tree = tb.parse(
      """
        |import eu.stratosphere.emma.api._
        |import eu.stratosphere.emma.ir._
        |
        |def predicate1(x: Int) = x <= 2
        |def predicate2(x: Int) = x > 5
        |
        |eu.stratosphere.emma.macros.testmacros.parallelize {
        | val alg = for (x <- DataBag(1 to 10)
        |                if predicate1(x) && x > 0 && x != 4 if predicate2(x))
        |              yield x} """.stripMargin)

    val tree1 = tb.typecheck(tree)
    import tb.u._

    val filters = tree1 collect {case f@Apply(TypeApply(Select(Select(q, TermName("Filter")), _), as), args)=> f}
    filters.size mustBe 4

    val ft = filters.flatMap(x => x collect {case Literal(t) => t})
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.!=(4)))")) mustBe true
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => predicate2(x)))")) mustBe true
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => x.>(0)))")) mustBe true
    ft.contains(Constant("(() => ((x: _root_.scala.Int) => predicate1(x)))")) mustBe true
  }
}
