package eu.stratosphere
package emma.codegen.spark

import emma.codegen.BaseCodegenTest
import emma.runtime.Spark

class CodegenTest extends BaseCodegenTest("spark") {

  override def runtimeUnderTest: Spark =
    Spark.testing()
}
