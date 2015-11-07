package eu.stratosphere.emma.codegen.flink

import eu.stratosphere.emma.codegen.BaseCodegenTest
import eu.stratosphere.emma.runtime.Flink

class CodegenTest extends BaseCodegenTest("flink") {
  override def runtimeUnderTest = new Flink()
}
