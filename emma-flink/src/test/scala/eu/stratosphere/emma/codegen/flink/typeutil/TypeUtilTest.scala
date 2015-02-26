package eu.stratosphere.emma.codegen.flink.typeutil

import org.junit.{Ignore, After, Before, Test}
import eu.stratosphere.emma.codegen.flink.testschema._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

class TypeUtilTest {

  val STC = SimpleTypeConvertor
  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  @Before def setup() {
  }


  @After def teardown(): Unit = {
  }

  @Test def testProductTypeFields(): Unit = {
    createTypeConvertor(typeOf[(Edge[Long], String, EdgeWithLabel[Int, String])]) match {
      case tc: ProductTypeConvertor =>
        val exp = List(STC.Long, STC.Long, STC.String, STC.Int, STC.Int, STC.String)
        val act = tc.fields.toList
        assert(exp == act, "Bad product type fields")
      case _ =>
        assert(assertion = false, "Unexpected type convertor type")
    }
  }

  @Test def testProductFieldConversion1(): Unit = {
    createTypeConvertor(typeOf[Edge[Long]]) match {
      case tc: ProductTypeConvertor =>
        val tree = tb.typecheck(
          q"""
          {
            val x = eu.stratosphere.emma.codegen.flink.testschema.Edge[Long](1L, 2L)
            val y = x.src
            val z = x.dst
          }
          """)

        val exp = tb.typecheck(
          q"""
          {
            val x  = new ${tc.tgtType}(1L, 2L)
            val y = x.f0
            val z = x.f1
          }
          """)

        val act = tc.convertTermType(TermName("x"), tc.convertResultType(tree))

        assert(exp equalsStructure typecheck(act), "Bad converted tree")
      case _ =>
        assert(assertion = false, "Unexpected type convertor type")
    }
  }

  @Ignore
  @Test def testProductFieldConversion2(): Unit = {
    createTypeConvertor(typeOf[Edge[(Int, String)]]) match {
      case tc: ProductTypeConvertor =>
        val tree = tb.typecheck(
          q"""
          {
            val x = eu.stratosphere.emma.codegen.flink.testschema.Edge[(Int, String)]((1, "foo"), (2, "bar"))
            val y = x.src
            val z = x.dst
          }
          """)

        val exp = tb.typecheck(
          q"""
          {
            val x  = new ${tc.tgtType}(1, "foo", 2, "bar")
            val y = new (Int, String)(x.f0, x.f1)
            val z = new (Int, String)(x.f2, x.f3)
          }
          """)

        val act = tc.convertTermType(TermName("x"), tc.convertResultType(tree))

        assert(exp equalsStructure typecheck(act), "Bad converted tree")
      case _ =>
        assert(assertion = false, "Unexpected type convertor type")
    }
  }

  private def typecheck(tree: Tree) = tb.typecheck(tb.untypecheck(tree))
}
