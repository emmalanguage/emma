package eu.stratosphere.emma.runtime

import de.tuberlin.aura.client.api.AuraClient
import de.tuberlin.aura.core.config.{IConfig, IConfigFactory}
import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.ir.{FoldSink, TempSink, ValueRef, Write}
import eu.stratosphere.emma.macros.program.util.Counter
import eu.stratosphere.emma.runtime.Aura.{DataBagRef, ScalarRef}
import eu.stratosphere.emma.runtime.codegen.aura

case class Aura(host: String, port: Int) extends Engine {

  sys addShutdownHook {
    closeSession()
  }

  val tmpCounter = new Counter()

  val plnCounter = new Counter()

  val client = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT))

  private implicit def topologyBuilder = client.createTopologyBuilder()

  override def execute[A](root: FoldSink[A]): ValueRef[A] = {
    println("----------------------------------- fold sink START")

    client.submitTopology(aura.generate(root, nextPlnName), null)
    client.awaitSubmissionResult(1)

    println("----------------------------------- fold sink STOP")
    ScalarRef[A](root.name, this)
  }

  override def execute[A](root: TempSink[A]): ValueRef[DataBag[A]] = {
    println("----------------------------------- temp sink START")

    client.submitTopology(aura.generate(root, nextPlnName), null)
    client.awaitSubmissionResult(1)

    println("----------------------------------- temp sink STOP")
    DataBagRef[A](root.name, this)
  }

  override def execute[A](root: Write[A]): Unit = {
    println("----------------------------------- sink START")

    client.submitTopology(aura.generate(root, nextPlnName), null)
    client.awaitSubmissionResult(1)

    println("----------------------------------- sink STOP")
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

  override def closeSession() = if (client != null) client.closeSession()

  private def nextTmpName = f"tmp-${tmpCounter.advance.get}%05d-${client.clientSessionID}"

  private def nextPlnName = f"topology-${plnCounter.advance.get}%05d-${client.clientSessionID}"
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