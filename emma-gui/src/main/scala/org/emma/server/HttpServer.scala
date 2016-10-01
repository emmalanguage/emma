/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emma.server

import java.io.PrintStream

import org.apache.log4j.Logger
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.webapp.WebAppContext
import org.emma.config.ConfigReader
import org.emma.servlets.{CodeServlet, LogEventServlet}

object HttpServer {
  private[server] var LOGGER: Logger = Logger.getRootLogger
  private var server: Server = null

  @throws(classOf[Exception])
  def main(args: Array[String]) {
    System.setOut(createLoggingProxy(System.out))
    System.setErr(createLoggingProxy(System.err))
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
    } catch { case ex: Exception =>
        ex.printStackTrace()
    }
  }

}