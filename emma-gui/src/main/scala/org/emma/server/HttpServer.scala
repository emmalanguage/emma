package org.emma.server

import java.io.PrintStream

import org.apache.log4j.Logger
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.webapp.WebAppContext
import org.emma.config.ConfigReader
import org.emma.runtime.LocalRuntime
import org.emma.servlets.{CodeServlet, LogEventServlet}

object HttpServer {
  private[server] var LOGGER: Logger = Logger.getRootLogger
  private var server: Server = null

  @throws(classOf[Exception])
  def main(args: Array[String]) {
    LocalRuntime.getInstance
    System.setOut(createLoggingProxy(System.out))
    val server: HttpServer.type = HttpServer
    server.start()
  }

  def createLoggingProxy(realPrintStream: PrintStream): PrintStream = {
    new PrintStream(realPrintStream) {
      override def print(string: String) {
        realPrintStream.print(string)
        LOGGER.info(string)
      }
    }
  }

  def start(): Unit = {
    this.server = new Server(ConfigReader.getInt("port"))
    val context: WebAppContext = new WebAppContext
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
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

}