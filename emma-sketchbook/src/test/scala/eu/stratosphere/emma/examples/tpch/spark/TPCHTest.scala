package eu.stratosphere.emma.examples.tpch.spark

import eu.stratosphere.emma.examples.tpch.BaseTPCHTest
import eu.stratosphere.emma.runtime

class TPCHTest extends BaseTPCHTest("Spark") {

  override def runtimeUnderTest = runtime.factory("spark-local", "local[*]", 6123)
}
