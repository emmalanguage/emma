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
package compiler

import ast.JavaAST

import scala.tools.reflect.ToolBoxFactory

import java.io._
import java.net.URLClassLoader
import java.nio.file._

/** A reflection-based [[Compiler]]. */
class RuntimeCompiler(private var cgd: Path = RuntimeCompiler.codeGenDir)
  extends Compiler with JavaAST with Serializable {

  // Make sure that generated class directory exists
  Files.createDirectories(cgd)

  def codeGenDir: Path = cgd

  /** The generating Scala toolbox. */
  override lazy val tb = {
    val cl = getClass.getClassLoader
    val factory = new ToolBoxFactory[u.type](u) {
      val mirror = u.runtimeMirror(cl)
    }

    cl match {
      case urlCL: URLClassLoader =>
        // Append current classpath to the toolbox in case of a URL classloader
        // (fixes a bug in Spark)
        val cp = urlCL.getURLs.map(_.getFile).mkString(sys.props("path.separator"))
        factory.mkToolBox(options = s"-d $codeGenDir -cp $cp")
      case _ =>
        // Use toolbox without extra classpath entries otherwise
        factory.mkToolBox(options = s"-d $codeGenDir")
    }
  }

  //noinspection ScalaUnusedSymbol
  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit =
    out.writeUTF(codeGenDir.toString)

  //noinspection ScalaUnusedSymbol
  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit =
    cgd = Paths.get(in.readUTF())
}

object RuntimeCompiler {

  /** [[RuntimeCompiler]] defaults. */
  object default {

    val runMethod = "run"

    lazy val codeGenDir = Paths
      .get(sys.props("java.io.tmpdir"), "emma", "codegen")
      .toAbsolutePath
  }

  /** Get the code-gen dir from system properties. */
  def codeGenDir: Path = sys.props.get("emma.codegen.dir")
    .map(s => Paths.get(s).toAbsolutePath)
    .getOrElse(default.codeGenDir)
}
