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
package org.emmalanguage
package server

import config.ConfigReader

import org.apache.log4j.Logger
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.webapp.WebAppContext

import java.io.File
import java.io.PrintStream
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Path
import java.nio.file.Paths

object HttpServer {

  private final val EXAMPLE_DIR_DEFAULT = Paths
    .get(System.getProperty("user.dir"), "..", "emma-examples", "src", "main", "scala")
    .normalize().toAbsolutePath.toString
  private final val CODEGEN_DIR_DEFAULT = Paths
    .get(System.getProperty("java.io.tmpdir"), "emma", "codegen")
    .normalize().toAbsolutePath.toString

  private[server] var LOGGER: Logger = Logger.getRootLogger
  private var server: Server = null

  private[this] var graphPath: Option[Path] = None

  @throws(classOf[Exception])
  def main(args: Array[String]): Unit = {
    if (args.size != 1) {
      // FIXME make better error message
      println("Missing 1st argument (graph path)")
      System.exit(1)
    }
    graphPath = Some(Paths.get(args(0)))
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

    // require(exampleDir.isDirectory, s"Example folder `$exampleDir` does not exist")
    // require(codegenDir.isDirectory, s"Codegen folder `$codegenDir` does not exist")

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
    context.addServlet(new ServletHolder(new GraphServlet(graphPath.get)), "/graph/*")

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
