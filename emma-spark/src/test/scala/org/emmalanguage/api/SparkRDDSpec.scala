package org.emmalanguage
package api

import org.apache.spark.SparkContext

class SparkRDDSpec extends DataBagSpec {

  override type Bag[A] = SparkRDD[A]
  override type BackendContext = SparkContext

  override def withBackendContext[T](f: BackendContext => T): T =
    LocalSparkSession.withSparkContext(f)

  override def Bag[A: Meta](implicit sc: BackendContext): Bag[A] =
    SparkRDD[A]

  override def Bag[A: Meta](seq: Seq[A])(implicit sc: BackendContext): Bag[A] =
    SparkRDD(seq)
}
