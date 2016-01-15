package eu.stratosphere.emma.examples.text

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}


/**
 *
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over text files.
 *
 * The input is a plain text file with lines separated by newline characters and the
 * output is a file containing words and the number of times that they occure in the
 * input text file.
 *
 * Usage:
 * {{{
 *   WordCount <text path> <result path>>
 * }}}
 *
 * This example shows how to:
 *
 *   - write a very simple Emma program.
 *
 * @param inPath Base input path
 * @param outPath Output path
 */
class WordCount(inPath: String, outPath: String, rt: Engine) extends Algorithm(rt) {

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](WordCount.Command.KEY_INPUT),
    ns.get[String](WordCount.Command.KEY_OUTPUT),
    rt)

  def run() = {

    val alg =  emma.parallelize {

      // read the input files and split them into lowercased words
      val words = for {
        line <- read(inPath, new TextInputFormat[String]('\n'))
        word <- DataBag[String](line.toLowerCase.split("\\W+"))
      } yield word

      // group the words by their identity and count the occurrence of each word
      val counts = for {
        group <- words.groupBy[String] { identity }
      } yield (group.key, group.values.size)

      // write the results into a CSV file
      write(outPath,new CSVOutputFormat[(String, Long)])(counts)
    }

    alg.run(rt)
  }
}

object WordCount {

  class Command extends Algorithm.Command[WordCount] {

    // algorithm names
    override def name = "wordcount"

    override def description = "Word Count Example"

    override def setup(parser: Subparser) = {
      // basic setup
      super.setup(parser)

      // add arguments
      parser.addArgument(Command.KEY_INPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_INPUT)
        .metavar("INPATH")
        .help("base input file")
      parser.addArgument(Command.KEY_OUTPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_OUTPUT)
        .metavar("OUTPUT")
        .help("output file")
    }
  }

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
  }
}
