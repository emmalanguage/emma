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
package eu.stratosphere.emma.examples.graphs

import breeze.linalg._
import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

import scala.util.Random

class AlternatingLeastSquares(
    input:      String,
    output:     String,
    features:   Int,
    lambda:     Double,
    iterations: Int,
    rt:         Engine)
  extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.graphs.AlternatingLeastSquares.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns get AlternatingLeastSquares.Command.keyInput,
    ns get AlternatingLeastSquares.Command.keyOutput,
    ns get AlternatingLeastSquares.Command.keyFeatures,
    ns get AlternatingLeastSquares.Command.keyLambda,
    ns get AlternatingLeastSquares.Command.keyIterations,
    rt)

  val algorithm = emma.parallelize {
    // reusable variables and functions
    val eye = DenseMatrix.eye  [Double](features)
    val V0  = DenseVector.zeros[Double](features)
    val M0  = DenseMatrix.zeros[Double](features, features)
    val Vr  = { (V: DenseVector[Double], r: Double) => V * r   }.tupled
    val VVt = { (V: DenseVector[Double], _: Double) => V * V.t }.tupled

    // IO formats
    val inputFormat  = new CSVInputFormat [Rating]
    val outputFormat = new CSVOutputFormat[Rating]

    val ratings = for {
      g <- read(input, inputFormat).groupBy(_.identity)
    } yield {
      val u   = g.key._1
      val i   = g.key._2
      val rs  = g.values
      val avg = rs.map(_.value).sum / rs.size
      Rating(u, i, avg)
    } // read input and gather all ratings

    var users = for (g <- ratings.groupBy(_.user)) yield {
      val u  = g.key
      val Ru = g.values
      Vertex(u, Ru.size, V0)
    } // initialize users partition

    var items = for (g <- ratings.groupBy(_.item)) yield {
      val i   = g.key
      val Ri  = g.values
      val cnt = Ri.size
      val rnd = new Random(i.hashCode)
      val avg = Ri.map(_.value).sum / cnt
      val Fi  = Array.iterate(avg, features) { _ => rnd.nextDouble() }
      Vertex(i, cnt, DenseVector(Fi))
    } // initialize items partition

    // iterations alternate between the users/items partitions
    // all feature vectors are updated per iteration
    for (_ <- 1 to iterations) {
      users = for (user <- users) yield {
        val prefs = for { // derive preferences
          rating <- ratings
          if rating.user == user.id
          item   <- items
          if rating.item == item.id
        } yield item.features -> rating.value
        // calculate new feature vector
        val Vu = prefs.fold(V0)(Vr,  _ + _)
        val Au = prefs.fold(M0)(VVt, _ + _)
        val Eu = eye * (lambda * user.degree)
        user.copy(features = inv(Au + Eu) * Vu)
      } // update state of users

      items = for (item <- items) yield {
        val prefs = for { // derive preferences
          rating <- ratings
          if rating.item == item.id
          user   <- users
          if rating.user == user.id
        } yield user.features -> rating.value
        // calculate new feature vector
        val Vi = prefs.fold(V0)(Vr,  _ + _)
        val Ai = prefs.fold(M0)(VVt, _ + _)
        val Ei = eye * (lambda * item.degree)
        item.copy(features = inv(Ai + Ei) * Vi)
      } // update state of items
    }

    // calculate missing ratings
    val recommendations = for (user <- users; item <- items) yield
      Rating(user.id, item.id, user.features dot item.features)
    // write results to output
    write(output, outputFormat) { recommendations }
    recommendations
  }

  def run() = algorithm run rt
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
    type Vid = Long

    case class Rating(@id user: Vid, @id item: Vid, value: Double)
      extends Identity[(Vid, Vid)] { lazy val identity = user -> item }

    case class Vertex(@id id: Vid, degree: Long, features: DenseVector[Double])
      extends Identity[Vid] { def identity = id }
  }
}
