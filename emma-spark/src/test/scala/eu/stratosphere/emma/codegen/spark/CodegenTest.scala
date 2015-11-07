package eu.stratosphere.emma.codegen.spark

import eu.stratosphere.emma.codegen.BaseCodegenTest
import eu.stratosphere.emma.runtime.Spark

class CodegenTest extends BaseCodegenTest("spark") {
  override def runtimeUnderTest = new Spark()
}
