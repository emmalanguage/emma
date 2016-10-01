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

class AlternatingLeastSquares2(input: String, output: String, features: Int, lambda: Double,
    iterations: Int, rt: Engine) extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.graphs.AlternatingLeastSquares2._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get(AlternatingLeastSquares2.Command.input),
    ns.get(AlternatingLeastSquares2.Command.output),
    ns.get(AlternatingLeastSquares2.Command.features),
    ns.get(AlternatingLeastSquares2.Command.lambda),
    ns.get(AlternatingLeastSquares2.Command.iterations),
    rt)

  def run() = algorithm.run(rt)

  val algorithm = emma.parallelize {
    // reusable values
    val E = DenseMatrix.eye[Double](features)
    val V0 = Vector.zeros[Double](features)
    val M0 = DenseMatrix.zeros[Double](features, features)

    val ratings = for { // read input and gather all ratings
      dup <- read(input, new CSVInputFormat[Rating]).groupBy(_.identity)
    } yield { // if duplicate ratings present, take the average
      val sum = dup.values.map(_.value).sum
      val n = dup.values.size
      Rating(dup.key._1, dup.key._2, sum / n)
    }

    // initialize user features with zeros
    var users = for (perUser <- ratings.groupBy(_.user))
      yield User(perUser.key, V0)

    // initialize item features with random values
    var items = for (perItem <- ratings.groupBy(_.item)) yield {
      val random = new Random(perItem.key.hashCode)
      val Fi = Vector.fill(features) { random.nextDouble() }
      val sum = perItem.values.map(_.value).sum
      val ni = perItem.values.size
      Fi(0) = sum / ni
      Item(perItem.key, Fi)
    }

    // iterations alternate between the user / item features
    // all feature vectors are updated per iteration
    for (_ <- 1 to iterations) {
      // derive user preferences
      val userPrefs = for {
        user <- users
        rate <- ratings
        if rate.user == user.id
        item <- items
        if rate.item == item.id
      } yield Pref(user.id, item.features, rate.value)

      // update user features
      users = for (perUser <- userPrefs.groupBy(_.id)) yield {
        val nu = perUser.values.size
        val Vu = perUser.values.fold(V0)(_.scaledFeatures, _ + _)
        val Au = perUser.values.fold(M0)(_.crossFeatures, _ + _)
        val Eu = E * (lambda * nu)
        User(perUser.key, inv(Au + Eu) * Vu)
      }

      // derive item preferences
      val itemPrefs = for {
        item <- items
        rate <- ratings
        if rate.item == item.id
        user <- users
        if rate.user == user.id
      } yield Pref(item.id, user.features, rate.value)

      // update item features
      items = for (perItem <- itemPrefs.groupBy(_.id)) yield {
        val ni = perItem.values.size
        val Vi = perItem.values.fold(V0)(_.scaledFeatures, _ + _)
        val Ai = perItem.values.fold(M0)(_.crossFeatures, _ + _)
        val Ei = E * (lambda * ni)
        Item(perItem.key, inv(Ai + Ei) * Vi)
      }
    }

    // calculate missing ratings
    val recommendations = for (user <- users; item <- items)
      yield Rating(user.id, item.id, user.features.t * item.features)

    // write results to output
    write(output, new CSVOutputFormat[Rating]) { recommendations }
    recommendations
  }
}

object AlternatingLeastSquares2 {
  type Id = Long

  case class Rating(@id user: Id, @id item: Id, value: Double)
      extends Identity[(Id, Id)] { def identity = (user, item) }

  case class User(@id id: Id, features: Vector[Double])
      extends Identity[Id] { def identity = id }

  case class Item(@id id: Id, features: Vector[Double])
    extends Identity[Id] { def identity = id }

  case class Pref(@id id: Id, features: Vector[Double], rating: Double)
      extends Identity[Id] {
    def identity = id
    def scaledFeatures = features * rating
    def crossFeatures = {
      val rowMatrix = features.toDenseVector.toDenseMatrix
      rowMatrix.t * rowMatrix
    }
  }

  class Command extends Algorithm.Command[AlternatingLeastSquares2] {
    import Command._
    val name = "als2"
    val description = "Compute recommendations from a set of ratings."

    override def setup(parser: Subparser) = {
      super.setup(parser)

      parser.addArgument(input)
        .`type`(classOf[String])
        .dest(input)
        .metavar("INPUT")
        .help("ratings file")

      parser.addArgument(output)
        .`type`(classOf[String])
        .dest(output)
        .metavar("OUTPUT")
        .help("recommendations file")

      parser.addArgument(features)
        .`type`(classOf[Int])
        .dest(features)
        .metavar("FEATURES")
        .help("number of latent features")

      parser.addArgument(lambda)
        .`type`(classOf[Double])
        .dest(lambda)
        .metavar("LAMBDA")
        .help("regularization factor")

      parser.addArgument(iterations)
        .`type`(classOf[Int])
        .dest(iterations)
        .metavar("ITERATIONS")
        .help("number of iterations")
    }
  }

  object Command {
    val input = "input"
    val output = "output"
    val features = "features"
    val lambda = "lambda"
    val iterations = "iterations"
  }
}
