#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}
package cli

import algorithms.graphs._
import algorithms.graphs.model._
import algorithms.ml.clustering._
import algorithms.ml.model._
import algorithms.text._

import org.apache.flink.api.scala.ExecutionEnvironment
import org.emmalanguage.FlinkAware
import org.emmalanguage.api._

object FlinkRunner extends FlinkAware {

  // ---------------------------------------------------------------------------
  // Config and helper type aliases
  // ---------------------------------------------------------------------------

  //@formatter:off
  case class Config
  (
    // general parameters
    command     : Option[String]       = None,
    // union of all parameters bound by a command option or argument
    // (in alphabetic order)
    csv         : CSV                  = CSV(),
    epsilon     : Double               = 0,
    iterations  : Int                  = 0,
    input       : String               = System.getProperty("java.io.tmpdir"),
    D           : Int                  = 0,
    k           : Int                  = 0,
    output      : String               = System.getProperty("java.io.tmpdir")
  ) extends FlinkConfig
  //@formatter:on

  // ---------------------------------------------------------------------------
  // Parser & main method
  // ---------------------------------------------------------------------------

  val parser = new Parser {
    head("${parentArtifactId}", "${version}")

    help("help")
      .text("Show this help message")

    opt[String]("codegen")
      .text("custom codegen path")
      .action { (x, c) =>
        System.setProperty("emma.codegen.dir", x)
        c
      }

    section("Graph Analytics")
    cmd("transitive-closure")
      .text("Compute the transitive closure of a directed graph")
      .children(
        arg[String]("input")
          .text("edges path")
          .action((x, c) => c.copy(input = x)),
        arg[String]("output")
          .text("closure path")
          .action((x, c) => c.copy(output = x)))

    section("Machine Learning")
    cmd("k-means")
      .text("K-Means Clustering")
      .children(
        arg[Int]("D")
          .text("number of dimensions")
          .action((x, c) => c.copy(D = x))
          .validate(between("D", 0, Int.MaxValue)),
        arg[Int]("k")
          .text("number of clusters")
          .action((x, c) => c.copy(k = x))
          .validate(between("k", 0, Int.MaxValue)),
        arg[Double]("epsilon")
          .text("termination threshold")
          .action((x, c) => c.copy(epsilon = x))
          .validate(between("epsilon", 0, 1.0)),
        arg[Int]("iterations")
          .text("number of repeated iterations")
          .action((x, c) => c.copy(iterations = x))
          .validate(between("iterations", 0, 100)),
        arg[String]("input")
          .text("points path")
          .action((x, c) => c.copy(input = x)),
        arg[String]("output")
          .text("solution path")
          .action((x, c) => c.copy(output = x)))

    section("Text Analytics")
    cmd("word-count")
      .text("Word Count Example")
      .children(
        arg[String]("input")
          .text("documents path")
          .action((x, c) => c.copy(input = x)),
        arg[String]("output")
          .text("word counts path")
          .action((x, c) => c.copy(output = x)))
  }

  def main(args: Array[String]): Unit =
    (for {
      cfg <- parser.parse(args, Config())
      cmd <- cfg.command
      res <- cmd match {
        // Graphs
        case "transitive-closure" =>
          Some(transitiveClosure(cfg)(flinkEnv(cfg)))
        // Machine Learning
        case "k-means" =>
          Some(kMeans(cfg)(flinkEnv(cfg)))
        // Text
        case "word-count" =>
          Some(wordCount(cfg)(flinkEnv(cfg)))
        case _ =>
          None
      }
    } yield res) getOrElse parser.showUsage()

  // ---------------------------------------------------------------------------
  // Parallelized algorithms
  // ---------------------------------------------------------------------------

  // Graphs

  def transitiveClosure(c: Config)(implicit flink: ExecutionEnvironment): Unit =
    emma.onFlink {
      // read in set of edges to be used as input
      val edges = DataBag.readCSV[Edge[Long]](c.input, c.csv)
      // build the transitive closure
      val paths = TransitiveClosure(edges)
      // write the results into a file
      paths.writeCSV(c.output, c.csv)
    }

  // Machine Learning

  def kMeans(c: Config)(implicit flink: ExecutionEnvironment): Unit =
    emma.onFlink {
      // read the input
      val points = for (line <- DataBag.readText(c.input)) yield {
        val record = line.split("${symbol_escape}t")
        Point(record.head.toLong, record.tail.map(_.toDouble))
      }
      // do the clustering
      val solution = KMeans(c.D, c.k, c.epsilon, c.iterations)(points)
      // write the (pointID, clusterID) pairs into a file
      solution.map(s => (s.id, s.label.id)).writeCSV(c.output, c.csv)
    }

  // Text

  def wordCount(c: Config)(implicit flink: ExecutionEnvironment): Unit =
    emma.onFlink {
      // read the input files and split them into lowercased words
      val docs = DataBag.readText(c.input)
      // parse and count the words
      val counts = WordCount(docs)
      // write the results into a file
      counts.writeCSV(c.output, c.csv)
    }

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  class Parser extends scopt.OptionParser[Config]("${parentArtifactId}") {

    import scopt._

    import scala.Ordering.Implicits._

    override def usageExample: String =
      "${parentArtifactId} command [options] <args>..."

    override def cmd(name: String): OptionDef[Unit, Config] =
      super.cmd(name).action((_, c) => c.copy(command = Some(name)))

    def section(name: String): OptionDef[Unit, Config] =
      note(s"${symbol_escape}n${symbol_pound}${symbol_pound} ${symbol_dollar}name${symbol_escape}n")

    def between[T: Numeric](name: String, min: T, max: T): T => Either[String, Unit] =
      (x: T) =>
        if (x > min && x < max) success
        else failure(s"Value <${symbol_dollar}name> must be > ${symbol_dollar}min and < ${symbol_dollar}max")
  }

}
