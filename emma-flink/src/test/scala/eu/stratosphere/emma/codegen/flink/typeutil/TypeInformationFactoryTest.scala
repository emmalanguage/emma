package eu.stratosphere.emma.codegen.flink.typeutil

import eu.stratosphere.emma.codegen.flink.testschema._
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import org.junit.{After, Before, Test}

import scala.reflect.runtime.universe._

class TypeInformationFactoryTest {

  var compiler: DataflowCompiler = _
  var factory: TypeInformationFactory = _

  @Before def setup() {
    compiler = new DataflowCompiler()
    factory = new TypeInformationFactory(compiler)
  }

  @After def teardown(): Unit = {
  }

  @Test def integrationTest(): Unit = {

    type T1 = EdgeWithLabel[Int, String]
    type T2 = EdgeWithLabel[String, Int]

    // construct typeInfo instances
    val typeInfo1 = instantiate[EdgeWithLabel[Int, String]]
    val typeInfo2 = instantiate[EdgeWithLabel[String, Int]]

    val typeClass1 = typeInfo1.getTypeClass
    val typeClass2 = typeInfo2.getTypeClass

    val act1 = typeInfo1.createSerializer().asInstanceOf[CaseClassSerializer[T1]].createInstance(Array[AnyRef](Int.box(2), Int.box(7), "9"))
    val act2 = typeInfo2.createSerializer().asInstanceOf[CaseClassSerializer[T2]].createInstance(Array[AnyRef]("2", "7", Int.box(9)))

    val exp1 = new T1(2, 7, "9")
    val exp2 = new T2("2", "7", 9)

    // assert classes are correct
    assert(typeClass1 == classOf[T1], s"Unexpected class for T1: $typeClass1, expected ${classOf[T1]}")
    assert(typeClass2 == classOf[T2], s"Unexpected class for T2: $typeClass2, expected ${classOf[T2]}")
    // assert values are correct
    assert(act1 == exp1, s"Unexpected class for T1: $act1")
    assert(act2 == exp2, s"Unexpected class for T2: $act2")

    val xxx = new java.util.LinkedList[Int]()
    scala.collection.JavaConversions.collectionAsScalaIterable(xxx).toStream
  }

  def instantiate[T <: Product : TypeTag] = {
    compiler.tb.eval(factory.apply(typeOf[T])).asInstanceOf[CaseClassTypeInfo[T]]
  }

  @Test def tuplesTest(): Unit = {

    type T1 = (Int, Int)
    type T2 = (Int, Int, Int)

    // construct typeInfo instances
    val typeInfo1 = instantiate[(Int, Int)]
    val typeInfo2 = instantiate[(Int, Int, Int)]

    val typeClass1 = typeInfo1.getTypeClass
    val typeClass2 = typeInfo2.getTypeClass

    val act1 = typeInfo1.createSerializer().asInstanceOf[CaseClassSerializer[T1]].createInstance(Array[AnyRef](Int.box(2), Int.box(7)))
    val act2 = typeInfo2.createSerializer().asInstanceOf[CaseClassSerializer[T2]].createInstance(Array[AnyRef](Int.box(2), Int.box(7), Int.box(9)))

    val exp1 = new T1(2, 7)
    val exp2 = new T2(2, 7, 9)

    // assert classes are correct
    assert(typeClass1 == classOf[T1], s"Unexpected class for T1: $typeClass1, expected ${classOf[T1]}")
    assert(typeClass2 == classOf[T2], s"Unexpected class for T2: $typeClass2, expected ${classOf[T2]}")
    // assert values are correct
    assert(act1 == exp1, s"Unexpected class for T1: $act1")
    assert(act2 == exp2, s"Unexpected class for T2: $act2")
  }
}
