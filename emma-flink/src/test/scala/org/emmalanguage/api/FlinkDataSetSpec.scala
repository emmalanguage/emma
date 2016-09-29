package org.emmalanguage
package api

import org.apache.flink.api.scala.ExecutionEnvironment

class FlinkDataSetSpec extends DataBagSpec {

  override type Bag[A] = FlinkDataSet[A]
  override type BackendContext = ExecutionEnvironment

  override def withBackendContext[T](f: BackendContext => T): T =
    f(ExecutionEnvironment.getExecutionEnvironment)

  override def Bag[A: Meta](implicit flink: ExecutionEnvironment): Bag[A] =
    FlinkDataSet[A]

  override def Bag[A: Meta](seq: Seq[A])(implicit flink: ExecutionEnvironment): Bag[A] =
    FlinkDataSet(seq)
}
