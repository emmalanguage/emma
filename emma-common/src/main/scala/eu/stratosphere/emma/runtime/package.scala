package eu.stratosphere.emma

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logger
import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.ir._
import org.slf4j.LoggerFactory
import eu.stratosphere.emma.api.model.Identity

import scala.reflect.runtime.{universe=>ru}
import scala.reflect.runtime.universe._


package object runtime {

  case class Context(srcPositions: Set[(Int, Int)])

  // add new root file appender
  {
    val appender = new org.apache.log4j.RollingFileAppender()
    appender.setLayout(new org.apache.log4j.PatternLayout("%d{yy-MM-dd HH:mm:ss} [%p] %m%n"))
    appender.setFile(String.format("%s/emma.log", System.getProperty("emma.path.log", s"${System.getProperty("java.io.tmpdir")}/emma/log")), true, true, 4096)
    appender.setMaxFileSize("100KB")
    appender.setMaxBackupIndex(1)
    org.apache.log4j.Logger.getRootLogger.addAppender(appender)
  }

  private[emma] val logger = Logger(LoggerFactory.getLogger(classOf[Engine]))

  abstract class Engine {

    val envSessionID = UUID.randomUUID()

    private var closed = false

    var plugins: Seq[RuntimePlugin] = Seq.empty

    // log program run header
    {
      logger.info("############################################################")
      logger.info("# Emma: Parallel Dataflow Compiler")
      logger.info("############################################################")
      logger.info(s"Starting Emma session $envSessionID (${this.getClass.getSimpleName} runtime)")
    }

    val defaultDOP: Int

    def executeFold[A: TypeTag, B: TypeTag]
        (root: Fold[A, B], name: String, ctx: Context, closure: Any*): A

    def executeTempSink[A: TypeTag]
        (root: TempSink[A], name: String, ctx: Context, closure: Any*): DataBag[A]

    def executeWrite[A: TypeTag]
        (root: Write[A], name: String, ctx: Context, closure: Any*): Unit

    def executeStatefulCreate[A <: Identity[K]: TypeTag, K: TypeTag]
        (root: StatefulCreate[A, K], name: String, ctx: Context, closure: Any*)
        : AbstractStatefulBackend[A, K]

    final def closeSession() = if (!closed) {
      doCloseSession()
      closed = true
    }

    protected def doCloseSession() = logger.info(s"Closing Emma session $envSessionID (${this.getClass.getSimpleName} runtime)")
  }

  case class Native() extends Engine {

    logger.info(s"Initializing Native execution environment")

    sys addShutdownHook {
      closeSession()
    }

    override lazy val defaultDOP = 1

    override def executeFold[A: TypeTag, B: TypeTag]
        (root: Fold[A, B], name: String, ctx: Context, closure: Any*): A = ???

    override def executeTempSink[A: TypeTag]
        (root: TempSink[A], name: String, ctx: Context, closure: Any*): DataBag[A] = ???

    override def executeWrite[A: TypeTag]
        (root: Write[A], name: String, ctx: Context, closure: Any*): Unit = ???

    override def executeStatefulCreate[A <: Identity[K]: TypeTag, K: TypeTag]
        (root: StatefulCreate[A, K], name: String, ctx: Context, closure: Any*)
        : AbstractStatefulBackend[A, K] = ???
  }

  def default(): Engine = {
    val flinkIsAvailable = try {
        Class.forName("org.apache.flink.api.scala.ExecutionEnvironment", false, getClass.getClassLoader); true
      } catch {
        case _: Throwable => false
      }

    val sparkIsAvailable = try {
        Class.forName("org.apache.spark.SparkContext", false, getClass.getClassLoader); true
      } catch {
        case _: Throwable => false
      }

    if (flinkIsAvailable)
      factory("flink")
    else if (sparkIsAvailable)
      factory("spark")
    else
      Native()
  }

  def factory(name: String, plugins: Seq[RuntimePlugin] = Seq.empty) = {
    // compute class name
    val engineClazzName = s"${getClass.getPackage.getName}.${name.capitalize}"
    // reflect engine
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val engineClazz = mirror.staticClass(engineClazzName)
    val engineClazzMirror = mirror.reflectClass(engineClazz)
    val engineClassType = appliedType(engineClazz)

    require(engineClassType <:< typeOf[Engine], s"Cannot instantiate engine '$engineClazzName' (should implement Engine)")
    require(!engineClazz.isAbstract, s"Cannot instantiate engine '$engineClazzName' (cannot be abtract)")

    // find suitable constructor
    val constructor = engineClassType.decl(termNames.CONSTRUCTOR)
      .alternatives.collect {
        case c: MethodSymbol if !c.isPrivate => c -> c.typeSignature
      }
      .collectFirst {
        case (c, mt @ MethodType(Nil, _)) => c.asMethod
      }

    require(constructor.isDefined, s"Cannot find nullary constructor for engine '$engineClazzName'")

    // reflect engine constructor
    val constructorMirror = engineClazzMirror.reflectConstructor(constructor.get)
    // instantiate the Engine's default constructor
    val engine = constructorMirror().asInstanceOf[Engine]
    engine.plugins = plugins
    engine
  }
}
