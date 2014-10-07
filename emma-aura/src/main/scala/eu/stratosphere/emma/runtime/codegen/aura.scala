package eu.stratosphere.emma.runtime.codegen

import de.tuberlin.aura.core.dataflow.generator.TopologyGenerator
import de.tuberlin.aura.core.topology.Topology.AuraTopologyBuilder
import eu.stratosphere.emma.ir
import eu.stratosphere.emma.runtime.{operators => ops}

object aura {

  def generate[A](root: ir.Combinator[A], name: String)(implicit builder: AuraTopologyBuilder) = {
    val sinkDescriptor = wrapOperators(root).generateDescriptors
    new TopologyGenerator(builder).generate(sinkDescriptor).toTopology(name)
  }

  def wrapOperators(cur: ir.Combinator[_]): ops.aura.AuraOperator = cur match {
    case a: ir.Map[_, _] => ops.aura.Map(ops.MapOp(a), wrapOperators(a.xs))
    case a: ir.TempSource[_] => ops.aura.TempSource(ops.TempSourceOp(a))
    case a: ir.Write[_] => ops.aura.Sink(ops.SinkOp(a), wrapOperators(a.xs))
    case a: ir.TempSink[_] => ops.aura.TempSink(ops.TempSinkOp(a), wrapOperators(a.xs))
    case a: ir.Read[_] => ops.aura.Source(ops.SourceOp(a))
    case _ => throw new RuntimeException("Unsupported ir node")
  }
}
