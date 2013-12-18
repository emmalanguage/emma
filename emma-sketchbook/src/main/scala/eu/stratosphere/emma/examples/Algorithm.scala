package eu.stratosphere.emma.examples

import _root_.scala.io.Source
import _root_.scala.collection.JavaConversions._
import _root_.net.sourceforge.argparse4j.inf.Subparser

object Algorithm {

  // argument names
  val KEY_INPUT = "input-file"
  val KEY_OUTPUT = "output-file"
  val KEY_DIMENSIONS = "dimensions"

  // constants
  val DELIMITER = ","

  abstract class Config[A <: Algorithm](implicit val m: scala.reflect.Manifest[A]) {

    /**
     * Algorithm key.
     */
    val CommandName: String

    /**
     * Algorithm name.
     */
    val Name: String

    /**
     * Algorithm subparser configuration.
     *
     * @param parser The subparser for this algorithm.
     */
    def setup(parser: Subparser): Unit = {
      // add arguments
      parser.addArgument(Algorithm.KEY_INPUT)
        .`type`[String](classOf[String])
        .dest(Algorithm.KEY_INPUT)
        .metavar("INPUT")
        .help("input file")
      parser.addArgument(Algorithm.KEY_OUTPUT)
        .`type`[String](classOf[String])
        .dest(Algorithm.KEY_OUTPUT)
        .metavar("OUTPUT")
        .help("output file ")

      // add options (prefixed with --)
      parser.addArgument(s"--${Algorithm.KEY_DIMENSIONS}")
        .`type`[Integer](classOf[Integer])
        .dest(Algorithm.KEY_DIMENSIONS)
        .metavar("N")
        .help("input dimensions")

      // add defaults for options
      parser.setDefault(Algorithm.KEY_DIMENSIONS, new Integer(3))
    }

    /**
     * Create an instance of the algorithm.
     *
     * @param arguments The parsed arguments to be passed to the algorithm constructor.
     * @return
     */
    def instantiate(arguments: java.util.Map[String, Object]): Algorithm = {
      val constructor = m.runtimeClass.getConstructor(classOf[Map[String, Object]])
      constructor.newInstance(arguments.toMap).asInstanceOf[Algorithm]
    }
  }

}

abstract class Algorithm(val arguments: Map[String, Object]) {

  // common parameters for all algorithms
  val inputPath = arguments.get(Algorithm.KEY_INPUT).get.asInstanceOf[String]
  val outputPath = arguments.get(Algorithm.KEY_OUTPUT).get.asInstanceOf[String]

  val dimensions = arguments.get(Algorithm.KEY_DIMENSIONS).get.asInstanceOf[Int]

  def iterator() = new Iterator[(List[Double], List[Double], String)] {
    val reader = Source.fromFile(inputPath).bufferedReader()

    override def hasNext = reader.ready()

    override def next() = {
      val line = reader.readLine().split( """\""" + Algorithm.DELIMITER)
      val vector = (for (i <- 0 until dimensions) yield line(i).trim.toDouble).toList
      val center = (for (i <- dimensions until dimensions * 2) yield line(i).trim.toDouble).toList
      val identifier = line(dimensions * 2).trim
      (vector, center, identifier)
    }
  }

  def run(): Unit
}

