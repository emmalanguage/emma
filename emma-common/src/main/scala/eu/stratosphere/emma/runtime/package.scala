package eu.stratosphere.emma

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logger
import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.ir._
import org.slf4j.LoggerFactory
import eu.stratosphere.emma.api.model.Identity

import scala.reflect.runtime.universe._


package object runtime {

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

    // log program run header
    {
      logger.info("############################################################")
      logger.info("# Emma: Parallel Dataflow Compiler")
      logger.info("############################################################")
      logger.info(s"Starting Emma session $envSessionID (${this.getClass.getSimpleName} runtime)")
    }

    val defaultDOP: Int

    def executeFold[A: TypeTag, B: TypeTag](root: Fold[A, B], name: String, closure: Any*): A

    def executeTempSink[A: TypeTag](root: TempSink[A], name: String, closure: Any*): DataBag[A]

    def executeWrite[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit

    def executeStatefulCreate[A <: Identity[K]: TypeTag, K: TypeTag]
      (root: StatefulCreate[A, K], name: String, closure: Any*): AbstractStatefulBackend[A, K]

    def executeUpdateWithZero[S: TypeTag, K: TypeTag, B: TypeTag]
      (root: UpdateWithZero[S, K, B], name: String, closure: Any*): DataBag[B]

    def executeUpdateWithOne[S <: Identity[K]: TypeTag, K: TypeTag, A: TypeTag, B: TypeTag]
      (root: UpdateWithOne[S, K, A, B], name: String, closure: Any*): DataBag[B]

    def executeUpdateWithMany[S <: Identity[K]: TypeTag, K: TypeTag, A: TypeTag, B: TypeTag]
      (root: UpdateWithMany[S, K, A, B], name: String, closure: Any*): DataBag[B]

    final def closeSession() = if (!closed) {
      doCloseSession()
      closed = true
    }

    protected def doCloseSession() = logger.info(s"Closing Emma session $envSessionID (${this.getClass.getSimpleName} runtime)")
  }

  case class Native() extends Engine {

    logger.info(s"Initializing native execution environment")

    sys addShutdownHook {
      closeSession()
    }

    override lazy val defaultDOP = 1

    override def executeFold[A: TypeTag, B: TypeTag](root: Fold[A, B], name: String, closure: Any*): A = ???

    override def executeTempSink[A: TypeTag](root: TempSink[A], name: String, closure: Any*): DataBag[A] = ???

    override def executeWrite[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit = ???

    override def executeStatefulCreate[A <: Identity[K]: TypeTag, K: TypeTag]
      (root: StatefulCreate[A, K], name: String, closure: Any*): AbstractStatefulBackend[A, K] = ???

    override def executeUpdateWithZero[S: TypeTag, K: TypeTag, B: TypeTag]
      (root: UpdateWithZero[S, K, B], name: String, closure: Any*): DataBag[B] = ???

    override def executeUpdateWithOne[S <: Identity[K]: TypeTag, K: TypeTag, A: TypeTag, B: TypeTag]
      (root: UpdateWithOne[S, K, A, B], name: String, closure: Any*): DataBag[B] = ???

    override def executeUpdateWithMany[S <: Identity[K]: TypeTag, K: TypeTag, A: TypeTag, B: TypeTag]
      (root: UpdateWithMany[S, K, A, B], name: String, closure: Any*): DataBag[B] = ???
  }

  def default(): Engine = {
    val backend = System.getProperty("emma.execution.backend", "")
    val execmod = System.getProperty("emma.execution.mode", "")

    require(backend.isEmpty || ("native" :: "flink" :: "spark" :: Nil contains backend),
      "Unsupported execution backend (native|flink|spark).")
    require((backend.isEmpty || backend == "native") || ("local" :: "remote" :: Nil contains execmod),
      "Unsupported execution mode (local|remote).")

    backend match {
      case "flink" => execmod match {
        case "local" /* */ => factory("flink-local", "localhost", 6123)
        case "remote" /**/ => factory("flink-remote", "localhost", 6123)
      }
      case "spark" => execmod match {
        case "local" /* */ => factory("spark-local", s"local[${Runtime.getRuntime.availableProcessors()}]", 7077)
        case "remote" /**/ => factory("spark-remote", "localhost", 7077)
      }
      case _ => Native() // run native version of the code
    }
  }

  def factory(name: String, host: String, port: Int) = {
    // reflect engine
    val engineClazz = rootMirror.staticClass(s"${getClass.getPackage.getName}.${toCamelCase(name)}")
    val engineClazzMirror = rootMirror.reflectClass(engineClazz)
    val engineClassType = appliedType(engineClazz)

    if (!(engineClassType <:< typeOf[Engine]))
      throw new RuntimeException(s"Cannot instantiate engine '${getClass.getPackage.getName}.${toCamelCase(name)}' (should implement Engine)")

    if (engineClazz.isAbstract)
      throw new RuntimeException(s"Cannot instantiate engine '${getClass.getPackage.getName}.${toCamelCase(name)}' (cannot be abtract)")

    // reflect engine constructor
    val constructorMirror = engineClazzMirror.reflectConstructor(engineClassType.decl(termNames.CONSTRUCTOR).asMethod)
    // instantiate engine
    constructorMirror(host, port).asInstanceOf[Engine]
  }

  def toCamelCase(name: String) = name.split('-').map(x => x.capitalize).mkString("")
}
