package eu.stratosphere.emma.examples

import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.examples.Algorithm.Command
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.ArgumentParserException
import net.sourceforge.argparse4j.internal.HelpScreenException
import resource._

import scala.collection.JavaConversions._
import scala.language.existentials
import scala.reflect.runtime.universe._

object CommandLineInterface {

  def main(_args: Array[String]): Unit = {

    // empty args calls help
    val args = if (_args.length == 0) Array[String]("-?") else _args

    // load available algorithms
    val algorithms = Map((for (c <- loadAlgorithms[Command[Algorithm]]()) yield c.name -> c): _*)

    // construct base argument parser
    val parser = argumentParser

    // format algorithm arguments with the arguments parser (in order of algorithm names)
    for {
      key <- algorithms.keySet.toSeq.sorted
      alg <- Some(algorithms(key))
    } {
      alg.setup(parser.addSubparsers().addParser(alg.name, true).help(alg.description))
    }

    try {
      // parse the arguments
      val ns = parser.parseArgs(args)

      // format general options as system properties
      System.setProperty("algorithm.name", ns.getString("algorithm.name"))

      // make sure that algorithm exists
      if (!algorithms.containsKey(System.getProperty("algorithm.name"))) {
        throw new RuntimeException(String.format("Unexpected algorithm '%s'", System.getProperty("algorithm.name")))
      }

      // set codegen dir if provided
      for (codegenDir <- Option(ns.getString("emma.codegen.dir"))) {
        System.setProperty("emma.codegen.dir", codegenDir)
      }

      // instantiate and run algorithm
      for {
        alg <- algorithms.get(System.getProperty("algorithm.name"))
      } alg.instantiate(ns).run()
    } catch {
      case e: HelpScreenException =>
        parser.handleError(e)
        System.exit(0)
      case e: ArgumentParserException =>
        parser.handleError(e)
        System.exit(-1);
      case e: Throwable =>
        System.err.println(String.format("Unexpected error: %s", e.getMessage))
        e.printStackTrace()
        System.exit(-1);
    }
  }

  def argumentParser = {
    val parser = ArgumentParsers.newArgumentParser("emma-examples", false)
      .defaultHelp(true)
      .description("A collection of Emma example programs.")
    parser.addSubparsers()
      .help("an algorithm to run")
      .dest("algorithm.name")
      .metavar("ALGORITHM")

    // general options
    parser.addArgument("-?")
      .action(Arguments.help)
      .help("show this help message and exit")
      .setDefault(Arguments.SUPPRESS)
    // codegen dir
    parser.addArgument(s"--codegen-dir")
      .`type`[String](classOf[String])
      .dest("emma.codegen.dir")
      .metavar("PATH")
      .help(s"codegen path (default '${DataflowCompiler.CODEGEN_DIR_DEFAULT}')")

    parser
  }

  def loadAlgorithms[A: TypeTag](): Seq[A] = {
    val fqn = typeOf[A] match /* get fully qualified name for type parameter 'A' */ {
      case TypeRef(_, sym, _) => sym.asClass.fullName
    }
    val res = Seq.newBuilder[A] // result accumulator
    for {
      inp <- managed(getClass.getResourceAsStream(s"/META-INF/services/$fqn")) // commands list
      fqn <- scala.io.Source.fromInputStream(inp).getLines() // fully-qualified class name
    } {
      res += Class.forName(fqn).newInstance().asInstanceOf[A]
    }
    res.result()
  }
}
