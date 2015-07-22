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

      //read the text file, producing a databag
      val text = read(inPath, new TextInputFormat[String]('\n'))

      //split the text into lowercased words and group them
      val wordGroups = text.flatMap(s => DataBag[String](s.toLowerCase.split("\\W+")))
        .groupBy(x => x)

      //iterate over all groups and output a {key,count} pair for each of them
      val wc = for(grp <- wordGroups) yield (grp.key, grp.values.count())

      //write the results into a CSV file
      write(outPath,new CSVOutputFormat[(String, Long)])(wc)
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
