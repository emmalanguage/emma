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

import compiler.RuntimeCompiler

import com.typesafe.config.Config

import scala.util.control.NonFatal

import java.io.File
import java.net.URL
import java.net.URLClassLoader

trait RuntimeCompilerAware {

  type Env

  val codegenDir = RuntimeCompiler.codeGenDir

  codegenDir.toFile.mkdirs()
  addToClasspath(codegenDir.toFile)

  sys.addShutdownHook({
    deleteRecursive(codegenDir.toFile)
  })

  val compiler: RuntimeCompiler

  import compiler._
  import compiler.api._

  def Env: u.Type

  def transformations(cfg: Config): Seq[TreeTransform]

  def execute[T](e: u.Expr[T]): Env => T =
    execute(loadConfig(baseConfig))(e)

  def execute[T](config: String)(e: u.Expr[T]): Env => T =
    execute(loadConfig(config +: baseConfig))(e)

  def execute[T](cfg: Config)(e: u.Expr[T]): Env => T = {
    // construct the compilation pipeline
    val xfms = transformations(cfg) :+ addContext
    // construct the eval function
    val eval = cfg.getString("emma.compiler.eval") match {
      case "naive" => NaiveEval(pipeline(typeCheck = true)(xfms: _*)) _
      case "timer" => TimerEval(pipeline(typeCheck = true)(xfms: _*)) _
    }
    // apply the pipeline to the input tree
    val rslt = eval(e.tree)
    // optionally, print the result
    if (cfg.getBoolean("emma.compiler.print-result")) {
      warning(Tree.show(rslt), e.tree.pos)
    }
    // evaluate the resulting tree
    compiler.eval(rslt)
  }

  protected lazy val addContext = TreeTransform("RuntimeCompilerAware.addContext", tree => {
    // This is roughly equivalent to the following:
    //import u.Quasiquote
    //q"(env: $Env) => { implicit val e: $Env = env; $tree }"

    // Note that instead of making an implicit ValDef inside the lambda,
    // it would be good to make the parameter of the lambda implicit.
    // However, showCode prints such code incorrectly. See
    // https://github.com/scala/bug/issues/10936

    val envSym = compiler.api.ParSym(compiler.api.Owner.encl, compiler.api.TermName.fresh("env"), Env)
    val eValDef = compiler.api.ValDef(
      compiler.api.ValSym(compiler.api.Owner.encl, compiler.api.TermName.fresh("e"), Env, u.Flag.IMPLICIT),
      compiler.api.ParRef(envSym)
    )
    val body = compiler.api.Block(Seq(eValDef), tree)
    compiler.api.Lambda(Seq(envSym), body)
  })

  /** Adds a [[File]] to the classpath. */
  protected def addToClasspath(f: File): Unit =
    addToClasspath(f.toURI.toURL)

  /** Adds a [[URL]] to the classpath. */
  protected def addToClasspath(u: URL): Unit = {
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

  /** Deletes a file recursively. */
  protected def deleteRecursive(path: java.io.File): Boolean = {
    val ret = if (path.isDirectory) {
      path.listFiles().toSeq.foldLeft(true)((_, f) => deleteRecursive(f))
    } else /* regular file */ {
      true
    }
    ret && path.delete()
  }
}
