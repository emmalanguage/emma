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

import compiler.Compiler
import compiler.RuntimeCompiler

import scala.util.control.NonFatal

import java.io.File
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Paths
import java.util.UUID

trait RuntimeCompilerAware {

  val codegenFile = Paths.get("tmp", "emma", "codegen").toFile

  codegenFile.mkdirs()
  addToClasspath(codegenFile)

  val compiler: Compiler

  import compiler._
  import compiler.api._

  def Env: u.Type

  def evaluate: u.Expr[Any] => u.Tree

  def addContext(tree: u.Tree): u.Tree = {
    import u.Quasiquote

    val Cls = TypeName(UUID.randomUUID().toString)
    val run = TermName(RuntimeCompiler.default.runMethod)
    val prs = for {
      sym <- Tree.closure(tree).toSeq.sortBy(_.name.toString)
      if !sym.isStatic
      if !sym.isVar
    } yield {
      val x = sym.name
      val T = sym.info
      q"val $x: $T"
    }

    q"""
    class $Cls {
      def $run(..$prs)(implicit env: $Env) = $tree
    }
    """
  }

  /** Adds a [[File]] to the classpath. */
  def addToClasspath(f: File): Unit =
    addToClasspath(f.toURI.toURL)

  /** Adds a [[URL]] to the classpath. */
  def addToClasspath(u: URL): Unit = {
    try {
      val clsldr = ClassLoader.getSystemClassLoader.asInstanceOf[URLClassLoader]
      val method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
      method.setAccessible(true)
      method.invoke(clsldr, u)
    } catch {
      case NonFatal(t) =>
        throw new java.io.IOException("Error, could not add URL to system classloader", t)
    }
  }
}
