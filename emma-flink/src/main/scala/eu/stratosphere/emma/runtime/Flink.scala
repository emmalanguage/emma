package eu.stratosphere.emma.runtime

import java.io.File
import java.net.URL

import eu.stratosphere.emma.api.{ParallelizedDataBag, DataBag}
import eu.stratosphere.emma.codegen.flink.DataflowGenerator
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir._
import eu.stratosphere.emma.util.Counter
import org.apache.flink.api.scala.ExecutionEnvironment

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
    constructExecutionPlanJson(name, root)
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    dataflowCompiler.execute[A](dataflowSymbol, Array[Any](env) ++ closure ++ localInputs(root))
  }

  override def executeTempSink[A: TypeTag](root: TempSink[A], name: String, closure: Any*): DataBag[A] = {
    constructExecutionPlanJson(name, root)
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    val expr = dataflowCompiler.execute[DataSet[A]](dataflowSymbol, Array[Any](env) ++ closure ++ localInputs(root))
    DataBag(root.name, expr, expr.collect())
  }

  override def executeWrite[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit = {
    constructExecutionPlanJson(name, root)
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    dataflowCompiler.execute[Unit](dataflowSymbol, Array[Any](env) ++ closure ++ localInputs(root))
  }

  override def scatter[A: TypeTag](values: Seq[A]): DataBag[A] = {
    // create fresh value reference
    val refName = nextTmpName
    // generate and execute a 'scatter' dataflow
    val dataflowSymbol = dataflowGenerator.generateScatterDef(refName)
    val expr = dataflowCompiler.execute[DataSet[A]](dataflowSymbol, Array[Any](env, values))
    // return the value reference
    DataBag(refName, expr, expr.collect())
  }

  override def gather[A: TypeTag](ref: DataBag[A]): DataBag[A] = {
    // generate and execute a 'scatter' dataflow
    val dataflowSymbol = dataflowGenerator.generateGatherDef(ref.asInstanceOf[ParallelizedDataBag[A, DataSet[A]]].name)
    dataflowCompiler.execute[DataBag[A]](dataflowSymbol, Array[Any](env, ref.asInstanceOf[ParallelizedDataBag[A, DataSet[A]]].repr))
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
    val jars = {
      val jar = new java.io.File(this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
      if (jar.exists() && jar.isFile) {
        Array[String](jar.toString)
      } else {
        Array.empty[String]
      }
    }
    val path = Array[URL](new File(dataflowCompiler.codeGenDir).toURI.toURL)

    logger.info(s" - jars: [ ${jars.mkString(", ")} ]")
    logger.info(s" - path: [ ${path.mkString(", ")} ]")

    ExecutionEnvironment.createRemoteEnvironment(host, port, jars, path)
  }
}