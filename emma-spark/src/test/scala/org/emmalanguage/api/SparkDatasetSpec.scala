package org.emmalanguage
package api

import org.apache.spark.sql.SparkSession

class SparkDatasetSpec extends DataBagSpec {

  override type Bag[A] = SparkDataset[A]
  override type BackendContext = SparkSession

  override def withBackendContext[T](f: BackendContext => T): T =
    LocalSparkSession.withSparkSession(f)

  override def Bag[A: Meta](implicit spark: SparkSession): Bag[A] =
    SparkDataset[A]

  override def Bag[A: Meta](seq: Seq[A])(implicit spark: SparkSession): Bag[A] =
    SparkDataset(seq)
}
