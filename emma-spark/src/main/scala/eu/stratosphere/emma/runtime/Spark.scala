package eu.stratosphere
package emma.runtime

import emma.api.DataBag
import emma.api.model.Identity
import emma.codegen.spark.DataflowGenerator
import emma.codegen.utils.DataflowCompiler
import emma.ir._

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.runtime.currentMirror

/**
 * Proxy runtime for Apache Spark.
 *
 * In this runtime [[DataBag]]s are backed by [[RDD]]s. Comprehensions are compiled to dataflow
 * programs and executed in parallel. Both local and remote environments are supported.
 *
 * @param sc The backing Spark execution context.
 */
class Spark(val sc: SparkContext) extends Engine {

  logger.info(s"Initializing Spark execution environment")
  sys.addShutdownHook(close())

  private val compiler = new DataflowCompiler(currentMirror)
  private val generator = new DataflowGenerator(compiler, sessionId)
  override lazy val defaultDoP = sc.defaultParallelism

  def this() = this {
    val conf = new SparkConf()
      .setAppName("Emma Spark App")
      .setIfMissing("spark.master", s"local[*]")
      // overwrite output files
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.driver.allowMultipleContexts", "true")

    new SparkContext(conf)
  }

  override def executeFold[A, B](
    fold: Fold[A, B],
    name: String,
    ctx: Context,
    closure: Any*): A = {

    for (p <- plugins) p.handleLogicalPlan(fold, name, ctx, closure: _*)
    val dataFlow = generator.generateDataflowDef(fold, name)
    val args = sc +: closure ++: localInputs(fold)
    compiler.execute[A](dataFlow, args.toArray)
  }

  def executeTempSink[A](
    sink: TempSink[A],
    name: String,
    ctx: Context,
    closure: Any*): DataBag[A] = {

    for (p <- plugins) p.handleLogicalPlan(sink, name, ctx, closure: _*)
    val dataFlow = generator.generateDataflowDef(sink, name)
    val args = sc +: closure ++: localInputs(sink)
    val rdd = compiler.execute[RDD[A]](dataFlow, args.toArray)
    DataBag(sink.name, rdd, rdd.collect())
  }

  def executeWrite[A](
    write: Write[A],
    name: String,
    ctx: Context,
    closure: Any*): Unit = {

    for (p <- plugins) p.handleLogicalPlan(write, name, ctx, closure: _*)
    val dataFlow = generator.generateDataflowDef(write, name)
    val args = sc +: closure ++: localInputs(write)
    compiler.execute[RDD[A]](dataFlow, args.toArray)
  }

  def executeStatefulCreate[A <: Identity[K], K](
    stateful: StatefulCreate[A, K],
    name: String,
    ctx: Context,
    closure: Any*): AbstractStatefulBackend[A, K] = {

    for (p <- plugins) p.handleLogicalPlan(stateful, name, ctx, closure: _*)
    val dataFlow = generator.generateDataflowDef(stateful, name)
    val args = sc +: closure ++: localInputs(stateful)
    compiler.execute[AbstractStatefulBackend[A, K]](dataFlow, args.toArray)
  }

  override protected def closeSession(): Unit = {
    super.closeSession()
    sc.stop()
  }
}

object Spark extends Engine.Factory {

  def apply(sc: SparkContext): Spark =
    new Spark(sc)

  override def default(): Spark =
    new Spark

  override def testing(): Spark = {
    val spark = default()
    spark.sc.setLogLevel("WARN")
    spark
  }
}
