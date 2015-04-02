package eu.stratosphere.emma.runtime

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.codegen.flink.DataflowGenerator
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir.{Fold, TempSink, ValueRef, Write, localInputs}
import eu.stratosphere.emma.runtime.Flink.DataBagRef
import eu.stratosphere.emma.util.Counter
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.reflect.runtime.universe._

abstract class Flink(val host: String, val port: Int) extends Engine {

  import org.apache.flink.api.scala._

  sys addShutdownHook {
    closeSession()
  }

  override lazy val defaultDOP = env.getParallelism

  val tmpCounter = new Counter()

  val plnCounter = new Counter()

  val env: ExecutionEnvironment

  val dataflowCompiler = new DataflowCompiler(mirror)

  val dataflowGenerator = new DataflowGenerator(dataflowCompiler, envSessionID)

  override def executeFold[A: TypeTag, B: TypeTag](root: Fold[A, B], name: String, closure: Any*): A = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    dataflowCompiler.execute[A](dataflowSymbol, Array[Any](env) ++ closure ++ localInputs(root))
  }

  override def executeTempSink[A: TypeTag](root: TempSink[A], name: String, closure: Any*): ValueRef[DataBag[A]] = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    val expr = dataflowCompiler.execute[DataSet[A]](dataflowSymbol, Array[Any](env) ++ closure ++ localInputs(root))
    DataBagRef[A](root.name, expr)
  }

  override def executeWrite[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    dataflowCompiler.execute[Unit](dataflowSymbol, Array[Any](env) ++ closure ++ localInputs(root))
  }

  override def scatter[A: TypeTag](values: Seq[A]): ValueRef[DataBag[A]] = {
    // create fresh value reference
    val refName = nextTmpName
    // generate and execute a 'scatter' dataflow
    val dataflowSymbol = dataflowGenerator.generateScatterDef(refName)
    val expr = dataflowCompiler.execute[DataSet[A]](dataflowSymbol, Array[Any](env, values))
    // return the value reference
    DataBagRef(refName, expr, Some(DataBag(values)))
  }

  override def gather[A: TypeTag](ref: ValueRef[DataBag[A]]): DataBag[A] = {
    // generate and execute a 'scatter' dataflow
    val dataflowSymbol = dataflowGenerator.generateGatherDef(ref.name)
    dataflowCompiler.execute[DataBag[A]](dataflowSymbol, Array[Any](env, ref.asInstanceOf[DataBagRef[A]].expr))
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
      ExecutionEnvironment.getExecutionEnvironment
    }
  }
}

object Flink {

  case class DataBagRef[A: TypeTag](name: String, expr: DataSet[A], var v: Option[DataBag[A]] = Option.empty[DataBag[A]]) extends ValueRef[DataBag[A]] {
    def value: DataBag[A] = v.getOrElse({
      v = Some(DataBag[A](expr.collect))
      v.get
    })
  }

}