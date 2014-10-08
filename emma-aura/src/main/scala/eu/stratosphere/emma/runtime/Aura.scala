package eu.stratosphere.emma.runtime

import de.tuberlin.aura.client.api.AuraClient
import de.tuberlin.aura.core.config.{IConfig, IConfigFactory}
import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.ir.{FoldSink, TempSink, ValueRef, Write}
import eu.stratosphere.emma.macros.program.util.Counter
import eu.stratosphere.emma.runtime.Aura.{DataBagRef, ScalarRef}

case class Aura(host: String, port: Int) extends Engine {

  sys addShutdownHook {
    closeSession()
  }

  val counter = new Counter()

  val client = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT))

  override def execute[A](root: FoldSink[A]): ValueRef[A] = {
    // TODO: execute plan

    // FIXME: map from root.id
    ScalarRef[A](root.name, this)
  }

  override def execute[A](root: TempSink[A]): ValueRef[DataBag[A]] = {
    // TODO: execute plan

    // FIXME: map from root.id
    DataBagRef[A](root.name, this)
  }

  override def execute[A](root: Write[A]): Unit = {
    // TODO: execute plan
  }

  override def scatter[A](values: Seq[A]): ValueRef[DataBag[A]] = {
    val ref = DataBagRef(nextTmpName, this, Some(DataBag(values))) // create fresh value reference
    client.broadcastDataset(ref.uuid, collection.JavaConversions.seqAsJavaList(values)) // broadcast dataset using the reference UUID
    ref // return the value refernce
  }

  override def gather[A](ref: ValueRef[DataBag[A]]): DataBag[A] = {
    DataBag(collection.JavaConversions.collectionAsScalaIterable(client.getDataset[A](ref.uuid)).toList)
  }

  override def put[A](value: A): ValueRef[A] = {
    val ref = ScalarRef(nextTmpName, this, Some(value)) // create fresh value reference
    client.broadcastDataset(ref.uuid, collection.JavaConversions.seqAsJavaList(Seq(value))) // broadcast singleton dataset using the reference UUID
    ref // return the value refernce
  }

  override def get[A](ref: ValueRef[A]): A = {
    collection.JavaConversions.collectionAsScalaIterable(client.getDataset[A](ref.uuid)).head
  }

  override def closeSession() = {
    if (client != null) client.closeSession()
  }

  private def nextTmpName = f"tmp-${counter.advance.get}%05d-${client.clientSessionID}"
}

object Aura {

  case class DataBagRef[A](name: String, rt: Aura, var v: Option[DataBag[A]] = Option.empty[DataBag[A]]) extends ValueRef[DataBag[A]] {
    def value: DataBag[A] = v.getOrElse({
      v = Some(rt.gather[A](this))
      v.get
    })
  }

  case class ScalarRef[A](name: String, rt: Aura, var v: Option[A] = Option.empty[A]) extends ValueRef[A] {
    def value: A = v.getOrElse({
      v = Some(rt.get[A](this))
      v.get
    })
  }

}
