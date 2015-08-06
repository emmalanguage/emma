package eu.stratosphere.emma.runtime

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.codegen.spark.DataflowGenerator
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir.{TempSink, Fold, Write, localInputs}
import eu.stratosphere.emma.util.Counter
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

abstract class Spark(val host: String, val port: Int) extends Engine {

  sys addShutdownHook {
    closeSession()
  }

  override lazy val defaultDOP = sc.defaultParallelism

  val sc: SparkContext

  val dataflowCompiler = new DataflowCompiler(runtimeMirror(getClass.getClassLoader))

  val dataflowGenerator = new DataflowGenerator(dataflowCompiler, envSessionID)

  override def executeFold[A: TypeTag, B: TypeTag](root: Fold[A, B], name: String, closure: Any*): A = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    dataflowCompiler.execute[A](dataflowSymbol, Array[Any](sc) ++ closure ++ localInputs(root))
  }

  override def executeTempSink[A: TypeTag](root: TempSink[A], name: String, closure: Any*): DataBag[A] = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    val rdd = dataflowCompiler.execute[RDD[A]](dataflowSymbol, Array[Any](sc) ++ closure ++ localInputs(root))
    DataBag(root.name, rdd, rdd.collect())
  }

  override def executeWrite[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    dataflowCompiler.execute[RDD[A]](dataflowSymbol, Array[Any](sc) ++ closure ++ localInputs(root))
  }

  override protected def doCloseSession() = {
    super.doCloseSession()
    sc.stop()
  }
}

case class SparkLocal(override val host: String, override val port: Int) extends Spark(host, port) {

  logger.info(s"Initializing local execution environment for Spark ")

  val conf = new SparkConf()
    .setAppName("Emma SparkLocal App")
    .setMaster(host)
    .set("spark.hadoop.validateOutputSpecs", "false") // overwrite output files
    .set("spark.driver.allowMultipleContexts", "true")

  override val sc = new SparkContext(conf)
}

case class SparkRemote(override val host: String, override val port: Int) extends Spark(host, port) {

  logger.info(s"Initializing remote execution environment for Spark ")

  val conf = new SparkConf()
    .setAppName("Emma SparkRemote App")
    .setMaster(s"$host:$port")

  override val sc = new SparkContext(conf)
}
