package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.api.model._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

import scala.util.Random

class AlternatingLeastSquares2(
      input:      String,
      output:     String,
      features:   Int,
      lambda:     Double,
      iterations: Int,
      rt:         Engine)
    extends Algorithm(rt) {
  import eu.stratosphere.emma.examples.graphs.AlternatingLeastSquares2.Schema._

  def this(ns: Namespace, rt: Engine) = this(
    ns get AlternatingLeastSquares2.Command.keyInput,
    ns get AlternatingLeastSquares2.Command.keyOutput,
    ns get AlternatingLeastSquares2.Command.keyFeatures,
    ns get AlternatingLeastSquares2.Command.keyLambda,
    ns get AlternatingLeastSquares2.Command.keyIterations,
    rt)

  def run() = algorithm run rt

  val algorithm = emma.parallelize {
    val _features = features
    val _lambda   = lambda
    // reusable values
    val E  = Mat eye features
    val V0 = Vector.fill(features          )(0.0)
    val M0 = Vector.fill(features, features)(0.0)
    // IO formats
    val inputFormat  = new CSVInputFormat [Rating]
    val outputFormat = new CSVOutputFormat[Rating]

    val ratings = for { // read input and gather all ratings
      gr <- read(input, inputFormat) groupBy { _.identity }
    } yield {
      val avg = gr.values.map(_.value).sum() / gr.values.count()
      Rating(gr.key._1, gr.key._2, avg)
    }

    // initialize users partition
    var users = for (gu <- ratings groupBy { _.user })
      yield Vertex(gu.key, gu.values.count(), V0)

    // initialize items partition
    var items = for (gi <- ratings groupBy { _.item }) yield {
      val rnd = new Random(gi.key.hashCode)
      val cnt = gi.values.count()
      val avg = gi.values.map(_.value).sum() / cnt
      val Fi  = Vector.iterate(avg, _features) { _ => rnd.nextDouble() }
      Vertex(gi.key, cnt, Fi)
    }

    // initialize dummy messages
    var messages = for (user <- users; if user.degree < 0)
      yield Message(user.id, user.degree, user.F, 0)

    // iterations alternate between the users / items partitions
    // all feature vectors are updated per iteration
    for (_ <- 1 to iterations) {
      messages = for { // derive preferences
        user   <- users
        rating <- ratings
        if rating.user == user.id
        item   <- items
        if rating.item == item.id
      } yield Message(user.id, user.degree, item.F, rating.value)

      users = for { // update state of users
        gum <- messages groupBy { m => m.dst -> m.degree }
      } yield { // calculate new feature vector
        val Vu = gum.values.fold[Vec](V0, m => m.F * m.rating, _ + _)
        val Au = gum.values.fold[Mat](M0, m => m.F outer m.F,  _ + _)
        val Eu = E * (_lambda * gum.key._2)
        Vertex(gum.key._1, gum.key._2, (Au + Eu) invMul Vu)
      }

      messages = for { // derive preferences
        item   <- items
        rating <- ratings
        if rating.item == item.id
        user   <- users
        if rating.user == user.id
      } yield Message(item.id, item.degree, user.F, rating.value)

      items = for { // update state of users
        gim <- messages groupBy { m => m.dst -> m.degree }
      } yield { // calculate new feature vector
        val Vi = gim.values.fold[Vec](V0, m => m.F * m.rating, _ + _)
        val Ai = gim.values.fold[Mat](M0, m => m.F outer m.F,  _ + _)
        val Ei = E * (_lambda * gim.key._2)
        Vertex(gim.key._1, gim.key._2, (Ai + Ei) invMul Vi)
      }
    }

    // calculate missing ratings
    val recommendations = for (user <- users; item <- items)
      yield Rating(user.id, item.id, user.F inner item.F)
    // write results to output
    write(output, outputFormat) { recommendations }
    recommendations
  }
}

object AlternatingLeastSquares2 {

  class Command extends Algorithm.Command[AlternatingLeastSquares2] {
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
    type Vec = Vector[Double]
    type Mat = Vector[Vector[Double]]

    case class Rating(@id user: Vid, @id item: Vid, value: Double)
        extends Identity[(Vid, Vid)] { def identity = user -> item }

    case class Vertex(@id id: Vid, degree: Long, F: Vec)
        extends Identity[Vid] { def identity = id }

    case class Message(@id dst: Vid, degree: Long, F: Vec, rating: Double)
        extends Identity[Vid] { def identity = dst }

    implicit class VecOps(val self: Vec) extends AnyVal {
      def +(that: Double) = self map { _ + that }
      def *(that: Double) = self map { _ * that }

      def +(that: Vec) = self zip that map { case (x, y) => x + y }
      def *(that: Vec) = self zip that map { case (x, y) => x * y }

      def inner(that: Vec) = (self * that).sum
      def outer(that: Vec) = self map { x => that map { _ * x } }
    }

    implicit class MatOps(val self: Mat) extends AnyVal {
      def +(that: Double) = self map { _ + that }
      def *(that: Double) = self map { _ * that }

      def +(that: Mat) = self zip that map { case (u, v) => u + v }
      def *(that: Mat) = self zip that map { case (u, v) => u * v }
      
      def rows = self.length
      def cols = if (self.isEmpty) 0 else self.head.length
      
      def invMul(that: Vec) = {
        import breeze.linalg._
        val matrix = DenseMatrix.create(rows, cols, self.flatten.toArray)
        val vector = DenseVector(that.toArray)
        (inv(matrix) * vector).toScalaVector()
      }
    }

    object Mat {
      def eye(n: Int) = Vector.range(0, n) map { i =>
        (Vector.fill(i)(0.0) :+ 1.0) ++ Vector.fill(n - i - 1)(0.0)
      }
    }
  }
}
