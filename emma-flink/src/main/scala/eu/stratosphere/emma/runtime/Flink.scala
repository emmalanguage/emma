package eu.stratosphere.emma.runtime

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.api.model.Identity
import eu.stratosphere.emma.codegen.flink.{DataflowGenerator, TempResultsManager}
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir._
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.reflect.runtime.universe._

class Flink(val env: ExecutionEnvironment) extends Engine {

  import org.apache.flink.api.scala._

  logger.info(s"Initializing Flink execution environment")

  def this() = this(ExecutionEnvironment.getExecutionEnvironment)

  sys addShutdownHook {
    closeSession()
  }

  override lazy val defaultDOP = env.getParallelism

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

  override def executeStatefulCreate[S <: Identity[K]: TypeTag, K: TypeTag]
      (root: StatefulCreate[S, K], name: String, closure: Any*): AbstractStatefulBackend[S, K] = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    val res = dataflowCompiler.execute[AbstractStatefulBackend[S, K]](dataflowSymbol, Array[Any](env, resMgr) ++ closure ++ localInputs(root))
    resMgr.gc()
    res
  }

  override def executeUpdateWithZero[S: TypeTag, K: TypeTag, B: TypeTag]
      (root: UpdateWithZero[S, K, B], name: String, closure: Any*): DataBag[B] = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    val expr = dataflowCompiler.execute[DataSet[B]](dataflowSymbol, Array[Any](env, resMgr) ++ closure ++ localInputs(root))
    resMgr.gc()
    DataBag(root.name, expr, expr.collect())
  }

  override def executeUpdateWithOne[S <: Identity[K]: TypeTag, K: TypeTag, A: TypeTag, B: TypeTag]
      (root: UpdateWithOne[S, K, A, B], name: String, closure: Any*): DataBag[B] = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    val expr = dataflowCompiler.execute[DataSet[B]](dataflowSymbol, Array[Any](env, resMgr) ++ closure ++ localInputs(root))
    resMgr.gc()
    DataBag(root.name, expr, expr.collect())
  }

  override def executeUpdateWithMany[S <: Identity[K]: TypeTag, K: TypeTag, A: TypeTag, B: TypeTag]
      (root: UpdateWithMany[S, K, A, B], name: String, closure: Any*): DataBag[B] = {
    val dataflowSymbol = dataflowGenerator.generateDataflowDef(root, name)
    val expr = dataflowCompiler.execute[DataSet[B]](dataflowSymbol, Array[Any](env, resMgr) ++ closure ++ localInputs(root))
    resMgr.gc()
    DataBag(root.name, expr, expr.collect())
  }
}