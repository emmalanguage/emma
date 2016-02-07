package eu.stratosphere.emma

import com.typesafe.scalalogging.slf4j.Logger

import org.apache.log4j
import org.slf4j.LoggerFactory

import scala.reflect.runtime.currentMirror

/** Provides runtime logging and factory methods. */
package object runtime {

  /** Root file appender. */
  val appender = {
    val appender = new log4j.RollingFileAppender
    val layout = new log4j.PatternLayout("%d{yy-MM-dd HH:mm:ss} [%p] %m%n")
    lazy val defaultLogDir = s"${sys.props("java.io.tmpdir")}/emma/log"
    val logDir = sys.props.getOrElse("emma.path.log", defaultLogDir)
    val logBufferSize = 4096
    appender.setLayout(layout)
    appender.setFile(s"$logDir/emma.log", true, true, logBufferSize)
    appender.setMaxFileSize("100KB")
    appender.setMaxBackupIndex(1)
    log4j.Logger.getRootLogger.addAppender(appender)
    appender
  }

  private[emma] val logger =
    Logger(LoggerFactory.getLogger(classOf[Engine]))

  /**
   * Picks the first runtime available on the classpath other than [[Native]].
   * If none is available defaults to [[Native]].
   *
   * @return A runtime instance with default configuration.
   */
  def default(): Engine =
    if (flinkIsAvailable) factory("flink").default()
    else if (sparkIsAvailable) factory("spark").default()
    else Native()

  /**
   * Picks the first runtime available on the classpath other than [[Native]].
   * If none is available defaults to [[Native]].
   *
   * @return A runtime instance optimized for testing.
   */
  def testing(): Engine =
    if (flinkIsAvailable) factory("flink").testing()
    else if (sparkIsAvailable) factory("spark").testing()
    else Native()

  /**
   * Looks up a runtime factory by name.
   *
   * @param name The (class-)name of the desired [[Engine]].
   * @return A factory for convenient runtime initialization.
   */
  def factory(name: String): Engine.Factory = {
    val modName = s"${getClass.getPackage.getName}.${name.capitalize}"
    val modSym = currentMirror.staticModule(modName)
    val module = currentMirror.reflectModule(modSym).instance
    lazy val error = s"object `$modSym` should implement `Engine.Factory`"
    require(module.isInstanceOf[Engine.Factory], error)
    module.asInstanceOf[Engine.Factory]
  }

  private def flinkIsAvailable = try {
    val env = "org.apache.flink.api.scala.ExecutionEnvironment"
    Class.forName(env, false, getClass.getClassLoader)
    true
  } catch {
    case _: ClassNotFoundException => false
  }

  private def sparkIsAvailable = try {
    val sc = "org.apache.spark.SparkContext"
    Class.forName(sc, false, getClass.getClassLoader)
    true
  } catch {
    case _: ClassNotFoundException => false
  }

  /**
   * Context available at execution time.
   * Passed e.g. to runtime plugins.
   *
   * @param srcPositions A [[Set]] of character offsets that specify identified comprehensions
   *                     as fragments in the original source code.
   */
  case class Context(srcPositions: Set[(Int, Int)])
}
