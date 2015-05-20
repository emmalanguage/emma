package eu.stratosphere.emma.examples.tpch.flink

import eu.stratosphere.emma.examples.tpch.BaseTPCHTest
import eu.stratosphere.emma.runtime

class TPCHTest extends BaseTPCHTest("Flink") {

  override def runtimeUnderTest = runtime.factory("flink-local", "localhost", 6123)
}
