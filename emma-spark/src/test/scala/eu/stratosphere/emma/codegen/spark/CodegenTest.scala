package eu.stratosphere.emma.codegen.spark

import eu.stratosphere.emma.codegen.BaseCodegenTest
import eu.stratosphere.emma.runtime.SparkLocal

class CodegenTest extends BaseCodegenTest("spark") {
  val cores = Runtime.getRuntime.availableProcessors
  override def runtimeUnderTest = SparkLocal(s"local[$cores]", 6123)
}
