package eu.stratosphere.emma.codegen.utils

import java.nio.file.{Files, Paths}

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{currentMirror => cm, universe => ru}
import scala.tools.reflect.ToolBox

class DataflowCompiler {

  import eu.stratosphere.emma.codegen.utils.DataflowCompiler._
  import eu.stratosphere.emma.runtime.logger

  // get the directory where the toolbox will generate the runtime-defined classes
  private val classGenDir = {
    val path = Paths.get(s"/${System.getProperty("emma.codegen.dir", s"${System.getProperty("java.io.tmpdir")}/emma/codegen")}")
    // make sure that generated class directory exists
    Files.createDirectories(path)
    path.toAbsolutePath.toString
  }

  val tb = cm.mkToolBox(options = s"-d $classGenDir")

  logger.info(s"Dataflow compiler will use '$classGenDir' as a target directory")

  /**
   * Compile an object using the compiler toolbox.
   *
   * @param tree The tree containing the object definition.
   * @return The ModuleSymbol of the defined object
   */
  def compile(tree: ImplDef, withSource: Boolean = true) = {
    val untypechecked = tb.untypecheck(tree)
    val symbol = tb.define(replaceIdents(untypechecked).asInstanceOf[ImplDef]).asModule
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
    val writer = new java.io.PrintWriter(s"$classGenDir/${symbol.fullName.replace('.', '/')}.scala")
    try writer.write(showCode(tree))
    finally writer.close()
  }

  /**
   * Instantiate and execute the run method of a compiled dataflow via reflection.
   *
   * @param dfSymbol The symbol of the compiled dataflow class.
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

  val CONSTRUCTOR = ru.termNames.CONSTRUCTOR
  val RUN_METHOD = ru.TermName("run")

  object replaceIdents extends Transformer with (Tree => Tree) {
    override def transform(tree: Tree): Tree = tree match {
      case ident@Ident(TermName(x)) => Ident(TermName(x))
      case x => super.transform(x)
    }

    override def apply(tree: Tree) = transform(tree)
  }

}
