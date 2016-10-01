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
package eu.stratosphere.emma.codegen.utils

import java.net.URLClassLoader
import java.nio.file.{Files, Paths}

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

class DataflowCompiler(val mirror: Mirror) {

  import eu.stratosphere.emma.codegen.utils.DataflowCompiler._
  import eu.stratosphere.emma.runtime.logger

  /** The directory where the toolbox will store runtime-generated code */
  val codeGenDir = {
    val path = Paths.get(System.getProperty("emma.codegen.dir", CODEGEN_DIR_DEFAULT))
    // make sure that generated class directory exists
    Files.createDirectories(path)
    path.toAbsolutePath.toString
  }

  /** The generating Scala toolbox */  
  val tb = {
    val cl = getClass.getClassLoader

    cl match {
      case urlCL: URLClassLoader =>
        // append current classpath to the toolbox in case of a URL classloader (fixes a bug in Spark)
        val cp = urlCL.getURLs.map(_.getFile).mkString(System.getProperty("path.separator"))
        mirror.mkToolBox(options = s"-d $codeGenDir -cp $cp")
      case _ =>
        // use toolbox without extra classpath entries otherwise
        mirror.mkToolBox(options = s"-d $codeGenDir")
    }
  }

  logger.info(s"Dataflow compiler will use '$codeGenDir' as a target directory")

  /**
   * Compile an object using the compiler toolbox.
   *
   * @param tree The tree containing the object definition.
   * @return The ModuleSymbol of the defined object
   */
  def compile(tree: ImplDef, withSource: Boolean = true) = {
    val symbol = tb.define(tb.parse(showCode(tree)).asInstanceOf[ImplDef])
    if (withSource) writeSource(symbol, tree)
    logger.info(s"Compiling '${symbol.name}' at '${symbol.fullName}'")
    symbol
  }

  /**
   * Writes the source code corresponding to the given AST `tree` next to its compiled version.
   *
   * @param symbol The symbol of the compiled AST.
   * @param tree The AST to be written.
   */
  def writeSource(symbol: Symbol, tree: ImplDef) = {
    val writer = new java.io.PrintWriter(s"$codeGenDir/${symbol.fullName.replace('.', '/')}.scala")
    try writer.write(showCode(tree))
    finally writer.close()
  }

  /**
   * Instantiate and execute the run method of a compiled dataflow via reflection.
   *
   * @param dfSymbol The symbol of the compiled dataflow module.
   * @param args The arguments to be passed to the run method.
   * @tparam T The type of the result
   * @return A result of type T.
   */
  def execute[T](dfSymbol: ModuleSymbol, args: Array[Any]) = {
    val dfMirror = tb.mirror.reflectModule(dfSymbol)
    val dfInstanceMirror = tb.mirror.reflect(dfMirror.instance)
    val dfRunMethodMirror = dfInstanceMirror.reflectMethod(dfInstanceMirror.symbol.typeSignature.decl(RUN_METHOD).asMethod)

    logger.info(s"Running dataflow '${dfSymbol.name}'")
    val result = dfRunMethodMirror.apply(args: _*).asInstanceOf[T]
    logger.info(s"Dataflow '${dfSymbol.name}' finished")
    result
  }
}

object DataflowCompiler {
  val RUN_METHOD = TermName("run")
  val CODEGEN_DIR_DEFAULT =
    Paths.get(System.getProperty("java.io.tmpdir"), "emma", "codegen").toAbsolutePath.toString
}
