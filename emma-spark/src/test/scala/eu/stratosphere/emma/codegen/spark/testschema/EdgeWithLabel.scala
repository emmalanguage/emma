package eu.stratosphere.emma.codegen.spark.testschema

case class EdgeWithLabel[VT, LT](src: VT, dst: VT, label: LT) {}
