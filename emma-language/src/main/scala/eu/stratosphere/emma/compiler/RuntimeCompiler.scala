package eu.stratosphere.emma
package compiler

import java.net.URLClassLoader
import java.nio.file.{Files, Paths}

import scala.language.implicitConversions
import scala.reflect.runtime
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

/** A reflection-based [[Compiler]]. */
class RuntimeCompiler extends Compiler with RuntimeUtil {

  import RuntimeCompiler._

  /** The directory where the toolbox will store runtime-generated code */
  val codeGenDir = {
    val path = Paths.get(System.getProperty("emma.codegen.dir", CODEGEN_DIR_DEFAULT))
    // make sure that generated class directory exists
    Files.createDirectories(path)
    path.toAbsolutePath.toString
  }

  val mirror = runtime.currentMirror

  /** The underlying universe object. */
  override val universe = mirror.universe

  // FIXME: as constructor parameter
  /** The generating Scala toolbox */
  override val tb = {
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
}

object RuntimeCompiler {
  val RUN_METHOD = TermName("run")
  val CODEGEN_DIR_DEFAULT =
    Paths.get(System.getProperty("java.io.tmpdir"), "emma", "codegen").toAbsolutePath.toString
}
