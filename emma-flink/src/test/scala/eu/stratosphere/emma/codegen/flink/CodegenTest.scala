package eu.stratosphere.emma.codegen.flink

import eu.stratosphere.emma.codegen.BaseCodegenTest
import eu.stratosphere.emma.runtime.FlinkLocal

class CodegenTest extends BaseCodegenTest("flink") {

  override def runtimeUnderTest = FlinkLocal("localhost", 6123)

}
