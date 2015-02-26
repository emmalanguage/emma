package eu.stratosphere.emma.runtime

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.codegen.flink.DataflowGenerator
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir.{FoldSink, TempSink, ValueRef, Write, localInputs}
import eu.stratosphere.emma.runtime.Flink.{DataBagRef, ScalarRef}
import eu.stratosphere.emma.util.Counter
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.io.RemoteCollectorImpl

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

abstract class Flink(val host: String, val port: Int) extends Engine {

  sys addShutdownHook {
    closeSession()
  }

  override lazy val defaultDOP = env.getDegreeOfParallelism

  val tmpCounter = new Counter()

  val plnCounter = new Counter()

  val env: ExecutionEnvironment

  val dataflowCompiler = new DataflowCompiler()

  val dataflowGenerator = new DataflowGenerator(dataflowCompiler)

  override def execute[A: TypeTag](root: FoldSink[A], name: String, closure: Any*): ValueRef[A] = {
    // client.submitTopology(flink.generate(root, nextDataflowName), null)
    // client.awaitSubmissionResult(1)
    ScalarRef[A](root.name, this)
  }

  override def execute[A: TypeTag](root: TempSink[A], name: String, closure: Any*): ValueRef[DataBag[A]] = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    dataflowCompiler.execute[JobExecutionResult](dataflowSymbol, Array[Any](env) ++ closure ++ localInputs(root))
    DataBagRef[A](root.name, this)
  }

  override def execute[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    dataflowCompiler.execute[JobExecutionResult](dataflowSymbol, Array[Any](env) ++ closure ++ localInputs(root))
  }

  override def scatter[A: TypeTag](values: Seq[A]): ValueRef[DataBag[A]] = {
    // create fresh value reference
    val ref = DataBagRef(nextTmpName, this, Some(DataBag(values)))
    // generate and execute a 'scatter' dataflow
    val dataflowSymbol = dataflowGenerator.generateScatterDef(ref.name)
    dataflowCompiler.execute[JobExecutionResult](dataflowSymbol, Array[Any](env, values))
    // return the value reference
    ref
  }

  override def gather[A: TypeTag](ref: ValueRef[DataBag[A]]): DataBag[A] = {
    // generate and execute a 'scatter' dataflow
    val dataflowSymbol = dataflowGenerator.generateGatherDef(ref.name)
    DataBag[A](dataflowCompiler.execute[Seq[A]](dataflowSymbol, Array[Any](env)))
  }

  override def put[A: TypeTag](value: A): ValueRef[A] = {
    // create fresh value reference
    val ref = ScalarRef(nextTmpName, this, Some(value))
    // generate and execute a 'scatter' dataflow
    val dataflowSymbol = dataflowGenerator.generateScatterDef(ref.name)
    dataflowCompiler.execute[String](dataflowSymbol, Array[Any](env, Seq(value).asJava))
    // return the value reference
    ref
  }

  override def get[A: TypeTag](ref: ValueRef[A]): A = {
    null.asInstanceOf[A]
    //collection.JavaConversions.collectionAsScalaIterable(client.getDataset[A](ref.uuid)).head FIXME
  }

  override def closeSession() = {
    super.closeSession()
    RemoteCollectorImpl.shutdownAll()
  }

  private def nextTmpName = f"emma$$temp${tmpCounter.advance.get}%05d"
}

case class FlinkLocal(override val host: String, override val port: Int) extends Flink(host, port) {

  logger.info(s"Initializing local execution environment for Flink ")

  override val env = ExecutionEnvironment.createLocalEnvironment()
}

case class FlinkRemote(override val host: String, override val port: Int) extends Flink(host, port) {

  logger.info(s"Initializing remote execution environment for Flink at $host:$port")

  override val env = {
    val path = new java.io.File(this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
    if (path.exists() && path.isFile) {
      logger.info(s"Passing jar location '${path.toString}' to remote environment")
      ExecutionEnvironment.createRemoteEnvironment(host, port, path.toString)
    } else {
      ExecutionEnvironment.createRemoteEnvironment(host, port)
    }
  }
}

object Flink {

  case class DataBagRef[A: TypeTag](name: String, rt: Flink, var v: Option[DataBag[A]] = Option.empty[DataBag[A]]) extends ValueRef[DataBag[A]] {
    def value: DataBag[A] = v.getOrElse({
      v = Some(rt.gather[A](this))
      v.get
    })
  }

  case class ScalarRef[A: TypeTag](name: String, rt: Flink, var v: Option[A] = Option.empty[A]) extends ValueRef[A] {
    def value: A = v.getOrElse({
      v = Some(rt.get[A](this))
      v.get
    })
  }

}