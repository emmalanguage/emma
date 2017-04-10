#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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
package ${package}
package cli

import graphs._
import graphs.model._
import ml.clustering._
import ml.model._
import text._

import breeze.linalg.{Vector => Vec}
import org.apache.spark.sql.SparkSession
import org.emmalanguage.api.Meta.Projections._
import org.emmalanguage.api._
import org.emmalanguage.io.csv._
import org.emmalanguage.util.Iso

import scala.reflect.ClassTag

object SparkExamplesRunner {

  // ---------------------------------------------------------------------------
  // Config and helper type aliases
  // ---------------------------------------------------------------------------

  //@formatter:off
  case class Config
  (
    // general parameters
    master      : String               = "local[*]",
    command     : Option[String]       = None,
    // union of all parameters bound by a command option or argument
    // (in alphabetic order)
    csv         : CSV                  = CSV(),
    epsilon     : Double               = 0,
    iterations  : Int                  = 0,
    input       : String               = System.getProperty("java.io.tmpdir"),
    k           : Int                  = 0,
    lambda      : Double               = 0,
    output      : String               = System.getProperty("java.io.tmpdir")
  )
  //@formatter:on

  // ---------------------------------------------------------------------------
  // Parser & main method
  // ---------------------------------------------------------------------------

  val parser = new Parser {
    head("${parentArtifactId}", "${version}")

    help("help")
      .text("Show this help message")

    opt[String]("master")
      .action((x, c) => c.copy(master = x))
      .text("Spark master address")

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
        arg[Int]("k")
          .text("number of clusters")
          .action((x, c) => c.copy(k = x)),
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
          Some(transitiveClosure(cfg)(sparkSession(cfg)))
        // Machine Learning
        case "k-means" =>
          Some(kMeans(cfg)(sparkSession(cfg)))
        // Text
        case "word-count" =>
          Some(wordCount(cfg)(sparkSession(cfg)))
        case _ =>
          None
      }
    } yield res) getOrElse parser.showUsage()

  // ---------------------------------------------------------------------------
  // Parallelized algorithms
  // ---------------------------------------------------------------------------

  implicit def breezeVectorCSVConverter[V](implicit V: CSVColumn[V], ctag: ClassTag[V])
  : CSVConverter[Vec[V]] = CSVConverter.iso[Array[V], Vec[V]](
    Iso.make(Vec.apply, _.toArray), implicitly)

  // Graphs

  def transitiveClosure(c: Config)(implicit spark: SparkSession): Unit =
    emma.onSpark {
      // read in set of edges to be used as input
      val edges = DataBag.readCSV[Edge[Long]](c.input, c.csv)
      // build the transitive closure
      val paths = TransitiveClosure(edges)
      // write the results into a file
      paths.writeCSV(c.output, c.csv)
    }

  // Machine Learning

  def kMeans(c: Config)(implicit spark: SparkSession): Unit =
    emma.onSpark {
      // read the input
      val points = for (line <- DataBag.readCSV[String](c.input, c.csv)) yield {
        val record = line.split("${symbol_escape}t")
        Point(record.head.toLong, Vec(record.tail.map(_.toDouble)))
      }
      // do the clustering
      val solution = KMeans(c.k, c.epsilon, c.iterations)(points)
      // write the result model into a file
      solution.writeCSV(c.output, c.csv)
    }

  // Text

  def wordCount(c: Config)(implicit spark: SparkSession): Unit =
    emma.onSpark {
      // read the input files and split them into lowercased words
      val docs = DataBag.readCSV[String](c.input, c.csv)
      // parse and count the words
      val counts = WordCount(docs)
      // write the results into a file
      counts.writeCSV(c.output, c.csv)
    }

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  private def sparkSession(c: Config): SparkSession = SparkSession.builder()
    .master(c.master)
    .appName(s"Emma example: c.command")
    .getOrCreate()

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
