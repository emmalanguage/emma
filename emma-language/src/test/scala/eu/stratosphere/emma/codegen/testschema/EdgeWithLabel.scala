package eu.stratosphere.emma.codegen.testschema

case class EdgeWithLabel[VT, LT](src: VT, dst: VT, label: LT) {}
