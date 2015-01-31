package eu.stratosphere.emma.codegen.flink.typeutil

import org.junit.{After, Before, Test}
import eu.stratosphere.emma.codegen.flink.TestSchema._

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
            val x = eu.stratosphere.emma.codegen.flink.TestSchema.Edge[Long](1L, 2L)
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

        val act = tb.typecheck(tb.untypecheck(tc.convertResultType(tc.convertTermType(TermName("x"), tree))))

        assert(exp equalsStructure act, "Bad converted tree")
      case _ =>
        assert(assertion = false, "Unexpected type convertor type")
    }
  }
}
