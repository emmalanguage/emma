package eu.stratosphere.emma.macros

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PredicateNormalizationTest extends FlatSpec with Matchers {

  val tb = mkToolbox(s"-cp $toolboxClasspath")
  import tb.u._

  val imports = """
    |import eu.stratosphere.emma.api._
    |import eu.stratosphere.emma.ir._
    |""".stripMargin

  "Predicate normalization" should "normalize predicates into CNF" in {
    val algorithm = """
      |eu.stratosphere.emma.macros.testmacros.parallelize {
      |  val alg = for {
      |    x <- DataBag(1 to 10)
      |    if !(x > 5 || (x < 2 && x == 0)) || (x > 5 || !(x < 2))
      |  } yield x
      |} """.stripMargin

    filtersIn (algorithm) should contain theSameElementsAs
      "(() => ((x: Int) => x.<(2).`unary_!`.||(x.==(0).`unary_!`).||(x.>(5))))" :: Nil
  }

  it should "separate filter conjunction into seperate filters" in {
    val algorithm = """
      |eu.stratosphere.emma.macros.testmacros.parallelize {
      |  val alg = for {
      |    x <- DataBag(1 to 10)
      |    if x > 5 && x > 0 && x != 4
      |  } yield x
      |} """.stripMargin

    filtersIn (algorithm) should contain theSameElementsAs
      "(() => ((x: Int) => x.!=(4)))" ::
      "(() => ((x: Int) => x.>(5)))" ::
      "(() => ((x: Int) => x.>(0)))" :: Nil
  }

  it should "separate filter conjunction and single predicate into separate filters" in {
    val algorithm = """
      |eu.stratosphere.emma.macros.testmacros.parallelize {
      |  val alg = for {
      |    x <- DataBag(1 to 10)
      |    if x > 5 && x > 0 && x != 4
      |    if x <= 2
      |  } yield x
      |}""".stripMargin

    filtersIn (algorithm) should contain theSameElementsAs
      "(() => ((x: Int) => x.!=(4)))" ::
      "(() => ((x: Int) => x.>(5)))" ::
      "(() => ((x: Int) => x.>(0)))" ::
      "(() => ((x: Int) => x.<=(2)))" :: Nil
  }

  it should "seperate filter conjunction with UDF predicate into seperate filters" in {
    val algorithm = """
      |eu.stratosphere.emma.macros.testmacros.parallelize {
      |  def predicate1(x: Int) = x <= 2
      |  def predicate2(x: Int) = x > 5
      |
      |  val alg = for {
      |    x <- DataBag(1 to 10)
      |    if predicate1(x) && x > 0 && x != 4
      |    if predicate2(x)
      |  } yield x
      |} """.stripMargin

    filtersIn (algorithm) should contain theSameElementsAs
      "(() => ((x: Int) => x.!=(4)))" ::
      "((predicate2: Int => Boolean) => ((x: Int) => predicate2(x)))" ::
      "(() => ((x: Int) => x.>(0)))" ::
      "((predicate1: Int => Boolean) => ((x: Int) => predicate1(x)))" :: Nil
  }

  def filtersIn(algorithm: String): List[String] =
    tb.typecheck(tb.parse(imports + algorithm)) collect {
      case q"$_.Filter.apply[$_](${Literal(Constant(f: String))}, ..$_)" => f
    }
}
