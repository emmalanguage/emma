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

import api.Meta.Projections._
import api._
import examples.graphs._
import examples.graphs.model._
import examples.ml.classification._
import examples.ml.clustering._
import examples.ml.model._
import examples.text._
import io.csv.CSV

import breeze.linalg.Vector
import org.apache.spark.sql.SparkSession

// TODO: migrate this to `emma-spark` once the dependency to `emma-examples` is inverted
object SparkExamples {

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
    nbModelType : NaiveBayes.ModelType = NaiveBayes.ModelType.Bernoulli,
    output      : String               = System.getProperty("java.io.tmpdir")
  )
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
    cmd("transitive-closure")
      .text("Compute the transitive closure of a directed graph")
      .children(
        arg[String]("input")
          .text("edges path")
          .action((x, c) => c.copy(input = x)),
        arg[String]("output")
          .text("closure path")
          .action((x, c) => c.copy(output = x)))
    note("")
    cmd("triangle-count")
      .text("Count the number of triangle cliques in a graph")
      .children(
        arg[String]("input")
          .text("edges path")
          .action((x, c) => c.copy(input = x)))

    section("Machine Learning")
    cmd("naive-bayes")
      .text("Naive Bayes classification")
      .children(
        arg[Double]("lambda")
          .text("termination threshold")
          .action((x, c) => c.copy(lambda = x)),
        arg[NaiveBayes.ModelType]("model-type")
          .text("'beroulli' or 'multinomial'")
          .action((x, c) => c.copy(nbModelType = x)),
        arg[String]("input")
          .text("training data path")
          .action((x, c) => c.copy(input = x)),
        arg[String]("output")
          .text("resulting model path")
          .action((x, c) => c.copy(output = x)))
    note("")
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
        case "triangle-count" =>
          Some(triangleCount(cfg)(sparkSession(cfg)))
        // Machine Learning
        case "naive-bayes" =>
          Some(naiveBayes(cfg)(sparkSession(cfg)))
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

  // Machine Learning

  def naiveBayes(c: Config)(implicit spark: SparkSession): Unit =
    emma.onSpark {
      // read the training data
      val data = for (line <- DataBag.readCSV[String](c.input, c.csv)) yield {
        val record = line.split(",").map(_.toDouble)
        LVector(record.head, Vector(record.slice(1, record.length)))
      }
      // run classification algorithm
      val model = NaiveBayes(c.lambda, c.nbModelType)(data)
      // write the result model into a file
      model.writeCSV(c.output, c.csv)
    }

  def kMeans(c: Config)(implicit spark: SparkSession): Unit =
    emma.onSpark {
      // read the input
      val points = for (line <- DataBag.readCSV[String](c.input, c.csv)) yield {
        val record = line.split("\t")
        Point(record.head.toLong, Vector(record.tail.map(_.toDouble)))
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

  class Parser extends scopt.OptionParser[Config]("emma-examples") {

    import scopt._
    import scala.Ordering.Implicits._

    implicit val naiveBayesModelTypeRead: scopt.Read[NaiveBayes.ModelType] =
      scopt.Read.reads(NaiveBayes.ModelType.withName)

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
