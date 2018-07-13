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
import util.Iso

import breeze.linalg.{Vector => Vec}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.reflect.ClassTag
import examples.text._

object LabyrinthExamplesRunner extends LabyrinthAware {
  // Text

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
    output      : String               = System.getProperty("java.io.tmpdir")
  ) extends FlinkConfig
  //@formatter:on

  // ---------------------------------------------------------------------------
  // Parser & main method
  // ---------------------------------------------------------------------------

  val parser = new Parser {
    head("emma-examples", "0.2-SNAPSHOT")

    help("help")
      .text("Show this help message")

    opt[String]("codegen")
      .text("custom codegen path")
      .action((x, c) => {
        System.setProperty("emma.codegen.dir", x)
        c
      })

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

  implicit def breezeVectorCSVConverter[V: CSVColumn : ClassTag]: CSVConverter[Vec[V]] =
    CSVConverter.iso[Array[V], Vec[V]](Iso.make(Vec.apply, _.toArray), implicitly)

  def wordCount(c: Config)(implicit flink: StreamExecutionEnvironment): Unit =

    emma.onLabyrinth {
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
