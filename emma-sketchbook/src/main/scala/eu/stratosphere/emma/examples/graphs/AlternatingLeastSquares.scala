package eu.stratosphere.emma.examples.graphs

import breeze.linalg._

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine

import net.sourceforge.argparse4j.inf.{ Namespace, Subparser }

import scala.util.Random

class AlternatingLeastSquares(
      input:      String,
      output:     String,
      features:   Int,
      lambda:     Double,
      iterations: Int,
      rt:         Engine)
    extends Algorithm(rt) {
  import eu.stratosphere.emma.examples.graphs.AlternatingLeastSquares._
  import Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns get AlternatingLeastSquares.Command.keyInput,
    ns get AlternatingLeastSquares.Command.keyOutput,
    ns get AlternatingLeastSquares.Command.keyFeatures,
    ns get AlternatingLeastSquares.Command.keyLambda,
    ns get AlternatingLeastSquares.Command.keyIterations,
    rt)

  def run() = {
    val algorithm = emma.parallelize {
      // algorithm parameters
      val _features = features
      val _lambda   = lambda
      // reusable variables and functions
      val E   = DenseMatrix.  eye[Double](features)
      val V0  = DenseVector.zeros[Double](features)
      val M0  = DenseMatrix.zeros[Double](features, features)
      val Vr  = { (V: Vector, r: Double) => V * r   }.tupled
      val VVt = { (V: Vector, _: Double) => V * V.t }.tupled
      // IO formats
      val inputFormat  = new CSVInputFormat [Rating]
      val outputFormat = new CSVOutputFormat[Rating]
      // read input and gather all ratings
      val ratings = for (gr <- read(input, inputFormat).groupBy(_.identity))
        yield {
          val (u, i) = gr.key
          val avg    = gr.values.map(_.value).sum / gr.values.count()
          Rating(u, i, avg)
        }

      // initialize users partition
      var users = for (gu <- ratings.groupBy(_.user))
        yield Vertex(gu.key, gu.values.count(), V0)

      // initialize items partition
      var items = for (gi <- ratings.groupBy(_.item))
        yield {
          val cnt = gi.values.count()
          val rnd = new Random(gi.key.hashCode)
          val avg = gi.values.map(_.value).sum / cnt
          val Fi  = Array.iterate(avg, _features) { _ => rnd.nextDouble() }
          Vertex(gi.key, cnt, DenseVector(Fi))
        }

      // iterations alternate between the users/items partitions
      // all feature vectors are updated per iteration
      for (_ <- 1 to iterations) {
        users = for (user <- users) yield {
          val prefs = for { // derive preferences
            rating <- ratings
            if rating.user == user.id
            item <- items
            if rating.item == item.id
          } yield (item.features, rating.value)
          // calculate new feature vector
          val Vu = prefs.fold[Vector](V0, Vr,  _ + _)
          val Au = prefs.fold[Matrix](M0, VVt, _ + _)
          val Eu = E * (_lambda * user.degree)
          user.copy(features = inv(Au + Eu) * Vu)
        } // update state of users

        items = for (item <- items) yield {
          val prefs = for { // derive preferences
            rating <- ratings
            if rating.item == item.id
            user <- users
            if rating.user == user.id
          } yield (user.features, rating.value)
          // calculate new feature vector
          val Vi = prefs.fold[Vector](V0, Vr,  _ + _)
          val Ai = prefs.fold[Matrix](M0, VVt, _ + _)
          val Ei = E * (_lambda * item.degree)
          item.copy(features = inv(Ai + Ei) * Vi)
        } // update state of items
      }

      // calculate missing ratings
      val recommendations = for {
        Vertex(u, _, fu) <- users
        Vertex(i, _, fi) <- items
      } yield Rating(u, i, fu dot fi)
      // write results to output
      write(output, outputFormat) { recommendations }
    }

    algorithm.run(rt)
  }
}

object AlternatingLeastSquares {

  class Command extends Algorithm.Command[AlternatingLeastSquares] {
    import Command._

    override def name = "als"

    override def description =
      "Compute recommendations from a list of ratings."

    override def setup(parser: Subparser) = {
      super.setup(parser)

      parser.addArgument(keyInput)
        .`type`(classOf[String])
        .dest(keyInput)
        .metavar("RATINGS")
        .help("ratings file")

      parser.addArgument(keyOutput)
        .`type`(classOf[String])
        .dest(keyOutput)
        .metavar("OUTPUT")
        .help("recommendations file")

      parser.addArgument(keyFeatures)
        .`type`(classOf[Int])
        .dest(keyFeatures)
        .metavar("FEATURES")
        .help("number of features per user/item")

      parser.addArgument(keyLambda)
        .`type`(classOf[Double])
        .dest(keyLambda)
        .metavar("LAMBDA")
        .help("lambda factor")

      parser.addArgument(keyIterations)
        .`type`(classOf[Int])
        .dest(keyIterations)
        .metavar("ITERATIONS")
        .help("number of iterations")
    }
  }

  object Command {
    val keyInput      = "input"
    val keyOutput     = "output"
    val keyFeatures   = "features"
    val keyLambda     = "lambda"
    val keyIterations = "iterations"
  }

  object Schema {
    type Vid    = Long
    type Vector = DenseVector[Double]
    type Matrix = DenseMatrix[Double]

    case class Rating(@id user: Vid, @id item: Vid, value: Double)
        extends Identity[(Vid, Vid)] { def identity = (user, item) }

    case class Vertex(@id id: Vid, degree: Long, features: Vector)
        extends Identity[Vid] { def identity = id }
  }
}
