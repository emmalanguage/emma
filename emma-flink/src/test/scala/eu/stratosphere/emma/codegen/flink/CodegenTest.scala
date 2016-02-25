package eu.stratosphere
package emma.codegen.flink

import emma.codegen.BaseCodegenTest
import emma.runtime.Flink

class CodegenTest extends BaseCodegenTest("flink") {

  override def runtimeUnderTest: Flink =
    Flink.testing()
}
