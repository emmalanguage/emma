package eu.stratosphere.emma.examples.imdb

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

class IMDb(input: String, output: String, rt: Engine) extends Algorithm(rt) {
  import IMDb._
  import Schema._
  import Ordering._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get(IMDb.Command.keyInput),
    ns.get(IMDb.Command.keyOutput),
    rt)

  def run() = algorithm.run(rt)

  val algorithm = emma.parallelize {
    // let's crunch some numbers first
    val numbers = DataBag(1 to 1000)

    // find pythagorean triangles
    val pythagoras = for {
      a <- numbers
      b <- numbers
      c <- numbers
      if a <= b && b <= c
      if a*a + b*b == c*c
    } yield (a, b, c)

    for { (a, b, c) <- pythagoras.fetch() }
      println(s"$a^2 + $b^2 == $c^2")

    // read in some real data
    val movies = read(s"$input/imdb.csv", new CSVInputFormat[Movie])
    val cannes = read(s"$input/canneswinners.csv", new CSVInputFormat[Winner])
    val berlin = read(s"$input/berlinalewinners.csv", new CSVInputFormat[Winner])

    // who won the festivals?
    val winners = for {
      movie <- movies
      winner <- cannes plus berlin
      if movie.title == winner.title
    } yield movie

    for { winner <- winners.bottom(10)(by { _.rating }) }
      println(winner)

    // who directed the winners?
    val filmographies = for {
      movie <- movies
      winner <- cannes plus berlin
      if movie.title == winner.title
    } yield winner.director -> movie

    // how many festivals did each director win?
    val festivalsWon = for {
      filmography <- filmographies
        .groupBy { case (director, movie) => director }
    } yield filmography.key -> filmography.values.size

    for { (director, wins) <- festivalsWon.fetch() }
      println(s"Director $director won $wins festivals.")

    // write the results
    pythagoras.writeCsv(s"$output/triangles")
    winners.writeCsv(s"$output/winners")
    festivalsWon.writeCsv(s"$output/festivalswon")
  }
}

object IMDb {
  class Command extends Algorithm.Command[IMDb] {
    import Command._

    override def name = "imdb"

    override def description =
      "Play around with IMDb movies and film festivals."

    override def setup(parser: Subparser) = {
      super.setup(parser)

      parser.addArgument(keyInput)
        .`type`(classOf[String])
        .dest(keyInput)
        .metavar("INPUT")
        .help("input directory")

      parser.addArgument(keyOutput)
        .`type`(classOf[String])
        .dest(keyOutput)
        .metavar("OUTPUT")
        .help("output directory")
    }
  }

  object Command {
    val keyInput  = "input"
    val keyOutput = "output"
  }

  object Schema {
    case class Movie(title: String, rating: Double, rank: Int, link: String, year: Int)
    case class Winner(year: Int, title: String, director: String, country: String)
  }
}
