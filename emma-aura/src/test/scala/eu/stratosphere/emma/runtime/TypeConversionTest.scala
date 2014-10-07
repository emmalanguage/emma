package eu.stratosphere.emma.runtime

import eu.stratosphere.emma.runtime.codegen.utils.TypeConversion
import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

import de.tuberlin.aura.core.record.tuples._
import com.esotericsoftware.reflectasm.FieldAccess
import de.tuberlin.aura.core.common.utils.ArrayUtils
import java.io.Serializable
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.util.List
import java.util.ArrayList
import java.util.Collections
import java.util.Arrays
import java.util.StringTokenizer
import de.tuberlin.aura.core.record.TypeInformation

class TypeConversionTest extends AssertionsForJUnit {

  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{currentMirror => cm}
  import scala.tools.reflect.ToolBox

  @Test def getRuntimeTypeTest(): Unit = {
    val tuple = new Tuple2("String", "String")
    val e1 = reify(tuple)

    val result1 = TypeConversion.typeInformation(e1.staticType)

    val strRef = "String"
    val e2 = reify(strRef)

    val result2: TypeInformation = TypeConversion.typeInformation(e2.staticType)

    val e3 = reify("const. String")

    val result3: TypeInformation = TypeConversion.typeInformation(e3.staticType)
    println("x")
  }
}
