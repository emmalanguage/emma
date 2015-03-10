package eu.stratosphere.emma

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logger
import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.ir.{Fold, TempSink, ValueRef, Write}
import org.slf4j.LoggerFactory

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

  val mirror = runtimeMirror(getClass.getClassLoader)

  abstract class Engine {

    val envSessionID = UUID.randomUUID()

    // log program run header
    {
      logger.info("############################################################")
      logger.info("# Emma: Parallel Dataflow Compiler")
      logger.info("############################################################")
      logger.info(s"Starting Emma session $envSessionID")
    }

    val defaultDOP: Int

    def executeFold[A: TypeTag, B: TypeTag](root: Fold[A, B], name: String, closure: Any*): A

    def executeTempSink[A: TypeTag](root: TempSink[A], name: String, closure: Any*): ValueRef[DataBag[A]]

    def executeWrite[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit

    def scatter[A: TypeTag](values: Seq[A]): ValueRef[DataBag[A]]

    def gather[A: TypeTag](ref: ValueRef[DataBag[A]]): DataBag[A]

    def closeSession() = {
      logger.info(s"Closing Emma session $envSessionID")
    }
  }

  case class Native() extends Engine {

    sys addShutdownHook {
      closeSession()
    }

    override lazy val defaultDOP = 1

    override def executeFold[A: TypeTag, B: TypeTag](root: Fold[A, B], name: String, closure: Any*): A = ???

    override def executeTempSink[A: TypeTag](root: TempSink[A], name: String, closure: Any*): ValueRef[DataBag[A]] = ???

    override def executeWrite[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit = ???

    override def scatter[A: TypeTag](values: Seq[A]): ValueRef[DataBag[A]] = ???

    override def gather[A: TypeTag](ref: ValueRef[DataBag[A]]): DataBag[A] = ???
  }

  def factory(name: String, host: String, port: Int) = {
    // reflect engine
    val engineClazz = mirror.staticClass(s"${getClass.getPackage.getName}.${toCamelCase(name)}")
    val engineClazzMirror = mirror.reflectClass(engineClazz)
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
