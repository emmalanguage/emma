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
package cli

import api._
import examples.graphs._
import examples.graphs.model._
import examples.text._
import util.Iso

import breeze.linalg.{Vector => Vec}
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

object SparkExamplesRunner extends SparkAware {

  // ---------------------------------------------------------------------------
  // Config and helper type aliases
  // ---------------------------------------------------------------------------

  //@formatter:off
  case class Config
  (
    // general parameters
    master       : String               = defaultSparkConfig.master,
    warehouseDir : String               = defaultSparkConfig.warehouseDir,
    command      : Option[String]       = None,
    // union of all parameters bound by a command option or argument
    // (in alphabetic order)
    csv          : CSV                  = CSV(),
    epsilon      : Double               = 0,
    iterations   : Int                  = 0,
    input        : String               = sys.props("java.io.tmpdir"),
    output       : String               = sys.props("java.io.tmpdir")
  ) extends SparkConfig {
    lazy val appName = s"Emma example: ${command.getOrElse("unknown")}"
  }
  //@formatter:on

  // ---------------------------------------------------------------------------
  // Parser & main method
  // ---------------------------------------------------------------------------

  val parser = new Parser {
    head("emma-examples", "0.2-SNAPSHOT")

    help("help")
      .text("Show this help message")

    opt[String]("master")
      .action((x, c) => c.copy(master = x))
      .text("Spark master address")

    section("Graph Analytics")
    cmd("connected-components")
      .text("Label undirected graph vertices with component IDs")
      .children(
        arg[String]("input")
          .text("edges path")
          .action((x, c) => c.copy(input = x)),
        arg[String]("output")
          .text("labeled vertices path")
          .action((x, c) => c.copy(output = x)))
    note("")
    cmd("triangle-count")
      .text("Count the number of triangle cliques in a graph")
      .children(
        arg[String]("input")
          .text("edges path")
          .action((x, c) => c.copy(input = x)))

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
        case "connected-components" =>
          Some(connectedComponents(cfg)(sparkSession(cfg)))
        case "triangle-count" =>
          Some(triangleCount(cfg)(sparkSession(cfg)))
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

  def connectedComponents(c: Config)(implicit spark: SparkSession): Unit =
    emma.onSpark {
      // read in set of edges to be used as input
      val edges = DataBag.readCSV[Edge[Long]](c.input, c.csv)
      // build the connected components
      val paths = ConnectedComponents(edges)
      // write the results into a file
      paths.writeCSV(c.output, c.csv)
    }

  def triangleCount(c: Config)(implicit spark: SparkSession): Unit =
    emma.onSpark {
      // convert a bag of directed edges into an undirected set
      val incoming = DataBag.readCSV[Edge[Long]](c.input, c.csv)
      val outgoing = incoming.map(e => Edge(e.dst, e.src))
      val edges = (incoming union outgoing).distinct
      // compute all triangles
      val triangles = EnumerateTriangles(edges)
      // count the number of enumerated triangles
      val triangleCount = triangles.size
      // print the result to the console
      println(s"The number of triangles in the graph is $triangleCount")
    }

  // Text

  def wordCount(c: Config)(implicit spark: SparkSession): Unit =
    emma.onSpark {
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

  class Parser extends scopt.OptionParser[Config]("emma-examples") {

    import scopt._

    import scala.Ordering.Implicits._

    override def usageExample: String =
      "emma-examples command [options] <args>..."

    override def cmd(name: String): OptionDef[Unit, Config] =
      super.cmd(name).action((_, c) => c.copy(command = Some(name)))

    def section(name: String): OptionDef[Unit, Config] =
      note(s"\n## $name\n")

    def between[T: Numeric](name: String, min: T, max: T) = (x: T) =>
      if (x > min && x < max) success
      else failure(s"Value <$name> must be > $min and < $max")
  }

}
