package eu.stratosphere.emma.codegen.flink.testschema

case class EdgeWithLabel[VT, LT](src: VT, dst: VT, label: LT) {}
