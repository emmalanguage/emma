package org.emma.server

import java.io.{File, PrintStream}
import java.net.{URL, URLClassLoader}
import java.nio.file.Paths

import org.apache.log4j.Logger
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.webapp.WebAppContext
import org.emma.config.ConfigReader
import org.emma.servlets.{CodeServlet, LogEventServlet}

object HttpServer {

  private final val EXAMPLE_DIR_DEFAULT = Paths
    .get(System.getProperty("user.dir"), "..", "emma-examples", "src", "main", "scala")
    .normalize().toAbsolutePath.toString
  private final val CODEGEN_DIR_DEFAULT = Paths
    .get(System.getProperty("java.io.tmpdir"), "emma", "codegen")
    .normalize().toAbsolutePath.toString

  private[server] var LOGGER: Logger = Logger.getRootLogger
  private var server: Server = null

  @throws(classOf[Exception])
  def main(args: Array[String]): Unit = {
    System.setOut(createLoggingProxy(System.out))
    System.setErr(createLoggingProxy(System.err))
    HttpServer.start()
  }

  def createLoggingProxy(realPrintStream: PrintStream): PrintStream = {
    new PrintStream(realPrintStream) {
      override def print(string: String): Unit = {
        realPrintStream.print(string)
        LOGGER.info(string)
      }
    }
  }

  def start(): Unit = {
    val exampleDir = Paths.get(System.getProperty("emma.example.dir", EXAMPLE_DIR_DEFAULT))
      .normalize().toAbsolutePath.toFile
    val codegenDir = Paths.get(System.getProperty("emma.codegen.dir", CODEGEN_DIR_DEFAULT))
      .normalize().toAbsolutePath.toFile

    codegenDir.mkdirs()

    require(exampleDir.isDirectory, s"Example folder `$exampleDir` does not exist")
    require(codegenDir.isDirectory, s"Codegen folder `$codegenDir` does not exist")

    HttpServer.addFile(codegenDir)
    HttpServer.addFile(exampleDir)

    this.server = new Server(ConfigReader.getInt("port"))
    val context = new WebAppContext
    val cl = Thread.currentThread().getContextClassLoader

    context.setClassLoader(cl)
    context.getInitParams.put("org.eclipse.jetty.servlet.Default.useFileMappedBuffer", "false")
    context.setContextPath("/")
    context.setResourceBase("public")
    context.setWelcomeFiles(Array[String]("index.html"))
    context.addServlet(new ServletHolder(new CodeServlet), "/code/*")
    context.addServlet(new ServletHolder(new PlanServlet), "/plan/*")
    context.addServlet(new ServletHolder(new LogEventServlet), "/log/*")
    this.server.setHandler(context)

    try {
      server.start()
      System.out.println("Started server at port: " + ConfigReader.getString("port"))
      server.join()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

  def addFile(f: File): Unit = {
    addURL(f.toURI.toURL)
  }

  def addURL(u: URL): Unit = {
    val sysloader = ClassLoader.getSystemClassLoader.asInstanceOf[URLClassLoader]
    val sysclass = classOf[URLClassLoader]

    try {
      val method = sysclass.getDeclaredMethod("addURL", classOf[URL])
      method.setAccessible(true)
      method.invoke(sysloader, u)
    } catch {
      case t: Throwable =>
        throw new java.io.IOException("Error, could not add URL to system classloader", t)
    }

  }

}