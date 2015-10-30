package eu.stratosphere.emma.codegen.spark

import eu.stratosphere.emma.codegen.BaseCodegenTest
import eu.stratosphere.emma.runtime.SparkLocal

class CodegenTest extends BaseCodegenTest("spark") {

  override def runtimeUnderTest = SparkLocal("local[1]", 6123)

}
