package eu.stratosphere.emma

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logger
import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.ir.{FoldSink, TempSink, ValueRef, Write}
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}


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

    // log program run header
    {
      logger.info("############################################################")
      logger.info("# Emma: Parallel Dataflow Compiler")
      logger.info("############################################################")
      logger.info(s"Starting Emma session $envSessionID")
    }

    val defaultDOP: Int

    def execute[A: TypeTag](root: FoldSink[A], name: String, closure: Any*): ValueRef[A]

    def execute[A: TypeTag](root: TempSink[A], name: String, closure: Any*): ValueRef[DataBag[A]]

    def execute[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit

    def scatter[A: TypeTag](values: Seq[A]): ValueRef[DataBag[A]]

    def gather[A: TypeTag](ref: ValueRef[DataBag[A]]): DataBag[A]

    def put[A: TypeTag](value: A): ValueRef[A]

    def get[A: TypeTag](ref: ValueRef[A]): A

    def closeSession() = {
      logger.info(s"Closing Emma session $envSessionID")
    }
  }

  case object Native extends Engine {

    override lazy val defaultDOP = 0

    def execute[A: TypeTag](root: FoldSink[A], name: String, closure: Any*): ValueRef[A] = ???

    def execute[A: TypeTag](root: TempSink[A], name: String, closure: Any*): ValueRef[DataBag[A]] = ???

    def execute[A: TypeTag](root: Write[A], name: String, closure: Any*): Unit = ???

    def scatter[A: TypeTag](values: Seq[A]): ValueRef[DataBag[A]] = ???

    def gather[A: TypeTag](ref: ValueRef[DataBag[A]]): DataBag[A] = ???

    def put[A: TypeTag](value: A): ValueRef[A] = ???

    def get[A: TypeTag](ref: ValueRef[A]): A = ???
  }

  def factory(name: String, host: String, port: Int) = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    // reflect engine
    val engineClazz = mirror.staticClass(s"${getClass.getPackage.getName}.${toCamelCase(name)}")
    val engineClazzMirror = mirror.reflectClass(engineClazz)
    val engineClassType = ru.appliedType(engineClazz)

    if (!(engineClassType <:< ru.typeOf[Engine]))
      throw new RuntimeException(s"Cannot instantiate engine '${getClass.getPackage.getName}.${toCamelCase(name)}' (shold implement Engine)")

    if (engineClazz.isAbstract)
      throw new RuntimeException(s"Cannot instantiate engine '${getClass.getPackage.getName}.${toCamelCase(name)}' (cannot be abtract)")

    // reflect engine constructor
    val constructorMirror = engineClazzMirror.reflectConstructor(engineClassType.decl(ru.termNames.CONSTRUCTOR).asMethod)
    // instantiate engine
    constructorMirror(host, port).asInstanceOf[Engine]
  }

  def toCamelCase(name: String) = name.split('-').map(x => x.capitalize).mkString("")
}
