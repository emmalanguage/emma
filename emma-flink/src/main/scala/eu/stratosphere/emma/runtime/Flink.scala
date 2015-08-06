package eu.stratosphere.emma.runtime

import java.io.File
import java.net.URL

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.codegen.flink.{TempResultsManager, DataflowGenerator}
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir.{Fold, TempSink, Write, localInputs}
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.reflect.runtime.universe._

abstract class Flink(val host: String, val port: Int) extends Engine {

  import org.apache.flink.api.scala._

  sys addShutdownHook {
    closeSession()
  }

  override lazy val defaultDOP = env.getParallelism

  val env: ExecutionEnvironment

  val dataflowCompiler = new DataflowCompiler(runtimeMirror(getClass.getClassLoader))

  val dataflowGenerator = new DataflowGenerator(dataflowCompiler, envSessionID)

  val resMgr = new TempResultsManager(dataflowCompiler.tb, envSessionID)

  override def executeFold[A: TypeTag, B: TypeTag](root: Fold[A, B], name: String, closure: Any*): A = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    val res = dataflowCompiler.execute[A](dataflowSymbol, Array[Any](env, resMgr) ++ closure ++ localInputs(root))
    resMgr.gc()
    res
  }

  override def executeTempSink[A: TypeTag](root: TempSink[A], name: String, closure: Any*): DataBag[A] = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    val expr = dataflowCompiler.execute[DataSet[A]](dataflowSymbol, Array[Any](env, resMgr) ++ closure ++ localInputs(root))
    resMgr.gc()
    DataBag(root.name, expr, expr.collect())
  }

  override def executeWrite[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    dataflowCompiler.execute[Unit](dataflowSymbol, Array[Any](env, resMgr) ++ closure ++ localInputs(root))
    resMgr.gc()
  }
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